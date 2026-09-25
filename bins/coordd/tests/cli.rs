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
    /// The node's own key, PKCS#8 DER. A committed voter attests the
    /// endpoint catalog with it, which is the only way a catalog becomes
    /// this domain's rather than anybody's.
    node_key: Vec<u8>,
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
            node_key: Vec::new(),
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
        self.issue_at(name, cluster, replica, role, 1)
    }

    /// The same, at a chosen key generation: what a replacement looks
    /// like before the configuration commits it (task-58).
    fn issue_at(
        &self,
        name: &str,
        cluster: [u8; 16],
        replica: u8,
        role: coord_types::wire_v1::PeerRole,
        incarnation: u64,
    ) -> (rcgen::Certificate, rcgen::KeyPair) {
        let identity = coord_node_issuer::NodeIdentity {
            cluster: coord_types::ids::ClusterId(cluster),
            node: coord_types::ids::ReplicaId([replica; 16]),
            incarnation: coord_types::ids::ReplicaIncarnation::new(incarnation).expect("positive"),
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
            // Loopback as well, because a catalog lists addresses a
            // process can actually dial and a test has no DNS. The name
            // a certificate is valid for is not what says which voter
            // presented it -- the node-identity URI above is -- so this
            // adds a way to reach the node and no authority at all.
            rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
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
    ca.node_key = key.serialize_der();
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", certificate.der())).expect("cert");
    std::fs::write(dir.join("roots.pem"), ca.root_pem()).expect("roots");
    let key_path = dir.join("node.key");
    let _ = std::fs::remove_file(&key_path);
    std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    collector_credential(dir, &ca, replica);
    ca
}

/// The credential this process presents when it submits to another
/// voter on a client's behalf.
///
/// A different certificate from the node's, because it is a different
/// principal: a node certificate binds one role, and the role that may
/// submit on a client's behalf is the collector's, not the voter's. It
/// names the same node, so an operator can still see which process it
/// is, and the genesis commits nothing about it -- what it proves is
/// that this domain's issuer said this process may act as a collector.
fn collector_credential(dir: &Path, ca: &Ca, replica: u8) {
    let (certificate, key) = ca.issue(
        SERVER_NAME,
        CLUSTER,
        replica,
        coord_types::wire_v1::PeerRole::Frontend,
    );
    std::fs::write(
        dir.join("collector.pem"),
        pem("CERTIFICATE", certificate.der()),
    )
    .expect("collector cert");
    let key_path = dir.join("collector.key");
    std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("collector key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

/// A signed endpoint catalog naming where each committed voter is.
///
/// Attested by voter 1, which is the node under test and the only one
/// whose key this fixture holds. That is exactly the rule: a catalog is
/// this domain's because one of its committed voters signed it, and an
/// address list nobody signed is nobody's.
///
/// `addresses` gives voter n's `host:port` list -- a node has two
/// listeners and a catalog entry is one list, because which of them
/// serves which plane is settled by dialling rather than declared. The
/// voters it omits are listed at an address nothing answers on, which
/// is what makes them unreachable rather than unknown.
fn endpoints(dir: &Path, ca: &Ca, count: u8, addresses: &[(u8, Vec<String>)]) {
    use coord_types::config_v1::{EndpointCatalogV1, EndpointV1, VoterSignatureV1};

    let endpoints: Vec<EndpointV1> = (1u8..=count)
        .map(|n| EndpointV1 {
            node: coord_types::ids::ReplicaId([n; 16]),
            incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("nonzero"),
            addresses: addresses
                .iter()
                .find(|(who, _)| *who == n)
                .map(|(_, a)| a.clone())
                // Port 1 on loopback: a real address that resolves
                // and that nothing is listening on, so this voter is
                // unreachable rather than unknown.
                .unwrap_or_else(|| vec!["127.0.0.1:1".to_owned()]),
            certificate_fingerprint: None,
        })
        .collect();
    let mut catalog = EndpointCatalogV1 {
        cluster: coord_types::ids::ClusterId(CLUSTER),
        domain: coord_types::ids::DomainId(DOMAIN),
        epoch: coord_types::ids::ConfigurationEpoch::new(1).expect("nonzero"),
        generation: coord_types::ids::EndpointGeneration::new(1).expect("nonzero"),
        endpoints,
        attestation: VoterSignatureV1 {
            node: coord_types::ids::ReplicaId([1; 16]),
            incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("nonzero"),
            signature: vec![0; 64],
        },
    };
    catalog.attestation.signature = coord_membership::configuration::sign_message(
        &jsonwebtoken::EncodingKey::from_ec_der(&ca.node_key),
        &catalog.catalog_message(),
    )
    .expect("attestation");
    std::fs::write(
        dir.join("endpoints.bin"),
        postcard::to_allocvec(&catalog).expect("catalog"),
    )
    .expect("write catalog");
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
/// The trust rule this test cluster's issuer signs under, written into
/// the domain's genesis policy by `coordd init`. One rule admits every
/// session the issuer mints: a rule is the issuer mapping, not a
/// session.
const TRUST_RULE: [u8; 16] = [0x7c; 16];

/// The principal every token in these tests names, and the one the
/// domain's genesis grants permissions to.
const PRINCIPAL: [u8; 16] = [0xa; 16];

/// The namespace those permissions cover, and the one every request
/// here is planned in.
const REQUEST_NAMESPACE: [u8; 16] = [0x5e; 16];

/// The single-use receipt identifier of `session`'s credential.
fn receipt_of(session: [u8; 16]) -> [u8; 32] {
    let mut out = [2u8; 32];
    out[..16].copy_from_slice(&session);
    out
}

fn service_token(ring: &coord_sts::KeyRing, session: [u8; 16]) -> String {
    let issued = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs();
    ring.sign(&coord_sts::ServiceClaims {
        iss: "https://sts.test".into(),
        sub: hex(&PRINCIPAL),
        aud: "control-plane-test".into(),
        sid: hex(&session),
        scope: 0xffff,
        rule: hex(&TRUST_RULE),
        generation: 1,
        // Per session, because a receipt is single use: the state
        // machine consumes it when it creates the session, and a second
        // session presenting the same one is correctly told the receipt
        // is spent. A fixture that minted one identifier for every
        // credential could establish exactly one session per domain.
        jti: hex(&receipt_of(session)),
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
    endpoints(dir, &ca, 3, &[]);
    let _ = sts_keys(dir);
    config_only(dir)
}

/// Just the TOML, for a caller that wrote the fixture itself.
fn config_only(dir: &Path) -> PathBuf {
    let text = format!(
        r#"config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
cluster_endpoints = "{root}/endpoints.bin"
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
collector_certificate = "{root}/collector.pem"
collector_key = "{root}/collector.key"

[sts]
issuer = "https://sts.test"
resource = "control-plane-test"
jwks = "{root}/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"

[[grant]]
principal = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"
namespace = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e"
"#,
        root = dir.display()
    );
    let path = dir.join("coordd.toml");
    std::fs::write(&path, text).expect("write config");
    path
}

/// Rewrite `config` so this node publishes a recovery checkpoint once
/// the journal is `after` records past its baseline.
///
/// The trigger is a local setting precisely so that it can be one: no
/// replicated result depends on when a node images its own storage, so
/// a test may ask for the cycle to happen now instead of waiting out
/// the four thousand records the default tolerates.
fn checkpoint_after(config: &Path, after: u64) {
    let mut text = std::fs::read_to_string(config).expect("read config");
    text.push_str(&format!(
        "\n[limits]\n\
         max_request_bytes = 2097152\n\
         max_response_bytes = 8388608\n\
         max_outstanding_per_session = 256\n\
         max_live_subscriptions = 4096\n\
         checkpoint_after_records = {after}\n"
    ));
    std::fs::write(config, text).expect("write config");
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

/// An initialization that failed before it finished can be run again,
/// and finishes -- including one that left a journal and no projection.
///
/// The two are separate directories, so no one write makes both exist.
/// Initialization now selects the projection before it creates the
/// journal (task-59), so one that cannot create the projection leaves
/// nothing behind. A journal and no projection is what an initialization
/// that created the journal first left (task-j08): without reusing it
/// the node was left with a journal and no projection, `init` refused
/// because a store existed, a start refused because none did, and the
/// only way out was deleting the journal by hand.
#[test]
fn an_initialization_that_stopped_between_the_journal_and_the_projection_can_be_finished() {
    let dir = workspace("half-init");
    let path = config(&dir);

    // Something that is not a directory where the projection goes: the
    // projection cannot be created, and so neither is the journal.
    std::fs::write(dir.join("state"), b"not a directory").expect("obstruct");
    let failed = run(&path, &["init"]);
    assert_eq!(failed.code, Some(2), "{}{}", failed.out, failed.err);
    assert!(
        !dir.join("journal").exists(),
        "a journal was created for a projection that was never selected"
    );
    std::fs::remove_file(dir.join("state")).expect("clear");

    // The journal an initialization that created it first left behind.
    drop(
        coord_journal_raft_engine::journal::RaftEngineJournal::create(
            &dir.join("journal"),
            coord_journal_raft_engine::journal::JournalIdentity {
                cluster: coord_types::ids::ClusterId(CLUSTER),
                replica: coord_types::ids::ReplicaId([1; 16]),
            },
            &coord_journal_raft_engine::journal::JournalOptions::default(),
        )
        .expect("a journal with no history"),
    );

    let finished = run(&path, &["init"]);
    assert_eq!(finished.code, Some(0), "{}{}", finished.out, finished.err);
    let report = start_and_report(&path);
    assert!(report.contains("owed=0"), "{report}");

    // Once the journal has been used, it is a store, and a second
    // initialization is refused as before.
    let again = run(&path, &["init"]);
    assert_eq!(again.code, Some(2), "{}{}", again.out, again.err);
    assert!(again.err.contains("already exists"), "{}", again.err);
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
    /// What the daemon has said since it came up. Runtime diagnostics go
    /// to stderr, and a test that wants to know whether a cluster formed
    /// has to be able to read them.
    said: std::sync::Arc<std::sync::Mutex<String>>,
    /// What it printed on stdout: its startup report.
    out: std::sync::Arc<std::sync::Mutex<String>>,
}

impl Running {
    /// Wait for the daemon to say something matching `needle`.
    fn waits_to_say(&self, needle: &str) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            if self.said.lock().expect("not poisoned").contains(needle) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    /// Everything it has said so far.
    fn said(&self) -> String {
        self.said.lock().expect("not poisoned").clone()
    }

    /// Wait up to `seconds` for what it has said to satisfy `test`.
    fn waits_until(&self, seconds: u64, test: impl Fn(&str) -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
        while std::time::Instant::now() < deadline {
            if test(&self.said.lock().expect("not poisoned")) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

/// The counts a daemon reported on its lines starting `prefix`, in order:
/// `peers connected=` or `voters submittable=`, each followed by `N of M`.
fn reported(said: &str, prefix: &str) -> Vec<usize> {
    said.lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .filter_map(|rest| rest.split(' ').next()?.parse().ok())
        .collect()
}

/// Whether a daemon's `prefix` count reached `full`, fell below it, and
/// came back to it: a link lost and dialled again.
fn healed(said: &str, prefix: &str, full: usize) -> bool {
    came_back(&reported(said, prefix), full)
}

/// The bulk-lane counts a daemon reported on its `peers connected=`
/// lines, in order.
fn reported_bulk(said: &str) -> Vec<usize> {
    said.lines()
        .filter(|line| line.starts_with("peers connected="))
        .filter_map(|line| line.split(' ').find_map(|w| w.strip_prefix("bulk=")))
        .filter_map(|n| n.parse().ok())
        .collect()
}

/// Whether `counts` reached `full`, fell below it, and came back to it.
fn came_back(counts: &[usize], full: usize) -> bool {
    let Some(up) = counts.iter().position(|n| *n == full) else {
        return false;
    };
    let Some(down) = counts[up..].iter().position(|n| *n < full) else {
        return false;
    };
    counts[up + down..].contains(&full)
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start `coordd` and leave it serving.
fn start(config: &Path) -> Running {
    start_with(config, &[])
}

/// Start `coordd` with `env` added to its environment and leave it
/// serving.
fn start_with(config: &Path, env: &[(&str, &str)]) -> Running {
    use std::io::{BufRead, BufReader};

    let mut child = Command::new(binary())
        .arg("--config")
        .arg(config)
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("coordd started");
    let stdout = child.stdout.take().expect("piped");
    let (tx, rx) = std::sync::mpsc::channel();
    let out = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let printed = std::sync::Arc::clone(&out);
    std::thread::spawn(move || {
        let mut api = None;
        let mut live = false;
        // Reads to the end rather than stopping at `live`. A daemon that
        // prints after it is live must not be killed by a reader that
        // stopped listening, and a test that let that happen would be
        // testing its own harness.
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            {
                let mut held = printed.lock().expect("not poisoned");
                held.push_str(&line);
                held.push('\n');
            }
            if let Some(rest) = line.strip_prefix("listening api_quic=") {
                api = rest.parse::<std::net::SocketAddr>().ok();
            }
            if !live && line.contains("phase=live") {
                live = true;
                let _ = tx.send(api);
            }
        }
        if !live {
            let _ = tx.send(None);
        }
    });
    let said = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let stderr = child.stderr.take().expect("piped");
    let collected = std::sync::Arc::clone(&said);
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let mut held = collected.lock().expect("not poisoned");
            held.push_str(&line);
            held.push('\n');
        }
    });
    let api = rx
        .recv_timeout(Duration::from_secs(30))
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            let _ = child.kill();
            let said = said.lock().expect("not poisoned").clone();
            panic!("coordd did not report a serving api listener within 30s:\n{said}")
        });
    Running {
        child,
        api,
        said,
        out,
    }
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
    _control_recv: quinn::RecvStream,
}

impl Caller {
    /// Dial `daemon`, negotiate as a client of this domain, and bind
    /// `session` with a token the daemon's own keys verify.
    async fn bind(daemon: &Running, ca: &Ca, ring: &coord_sts::KeyRing, session: [u8; 16]) -> Self {
        let (endpoint, connection, control, control_recv) = Self::negotiated(daemon, ca).await;
        let token = service_token(ring, session);
        let bind = coord_session::bind_frame(token.as_bytes()).expect("bind frame");
        let answer = ask(&connection, &bind)
            .await
            .unwrap_or_else(|| panic!("the daemon did not answer the binding:\n{}", daemon.said()));
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
            _control_recv: control_recv,
        }
    }

    /// Dial `daemon` and negotiate as a client of this domain, stopping
    /// before the binding. Separate from [`Caller::bind`] because a
    /// binding is now a replicated command, so a cluster that cannot
    /// agree does not acknowledge one -- and a test about that has to be
    /// able to reach the point of asking.
    async fn negotiated(
        daemon: &Running,
        ca: &Ca,
    ) -> (
        quinn::Endpoint,
        quinn::Connection,
        quinn::SendStream,
        quinn::RecvStream,
    ) {
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
        let (mut control, mut control_recv) = connection.open_bi().await.expect("control stream");
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

        // Wait for the acknowledgement before asking anything.
        //
        // Negotiation is per connection, and a request stream opened
        // before the node has bound this caller arrives at a connection
        // the node does not yet know. A real client waits, and a test
        // that did not would pass or fail on how busy the node happened
        // to be when it connected.
        let ack = tokio::time::timeout(Duration::from_secs(20), async {
            let mut reader = coord_types::wire_v1::FrameReader::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = control_recv
                    .read(&mut buf)
                    .await
                    .expect("readable")
                    .expect("the node closed the control stream");
                reader.push(&buf[..n]).expect("within the reader bound");
                if let Some(frame) = reader.next_frame().expect("a frame") {
                    return frame;
                }
            }
        })
        .await
        .expect("the node acknowledged the negotiation within the bound");
        assert!(
            matches!(
                coord_types::wire_v1::decode(&ack),
                Ok(coord_types::wire_v1::MessageV1::HelloAck(_))
            ),
            "the node answered the hello with something else: {ack:?}"
        );
        (endpoint, connection, control, control_recv)
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
            coord_types::wire_v1::RequestV1::new(self.invocation(sequence), &logical, 0, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded")
    }

    /// One `Range` of exactly `key`, as a client would send it.
    fn range(&self, sequence: u64, key: &[u8]) -> Vec<u8> {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            coord_types::ids::NamespaceId([0x5e; 16]),
            coord_types::logical_v1::CanonicalOperation::Range(coord_types::logical_v1::RangeOp {
                range: coord_types::logical_v1::KeyRange::exact(key.to_vec()),
                revision: None,
                limit: 0,
                keys_only: false,
                count_only: false,
            }),
        );
        logical.canonicalize();
        coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(self.invocation(sequence), &logical, 0, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded")
    }

    /// A bounded range read over `prefix`.
    ///
    /// A read is what shows a replica that has stopped keeping up: it
    /// is served from this node's own projection, so a frontend whose
    /// voter is behind answers from state that has stopped moving --
    /// or, for the replicated policy a binding is held against, does
    /// not answer at all.
    fn scan(&self, sequence: u64, prefix: &[u8]) -> Vec<u8> {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            coord_types::ids::NamespaceId([0x5e; 16]),
            coord_types::logical_v1::CanonicalOperation::Range(coord_types::logical_v1::RangeOp {
                // The prefix as a half-open interval, which is how
                // a canonical range says "everything under this".
                range: coord_types::logical_v1::KeyRange::interval(prefix.to_vec(), {
                    let mut end = prefix.to_vec();
                    let last = end.len() - 1;
                    end[last] += 1;
                    end
                }),
                revision: None,
                limit: 64,
                keys_only: false,
                count_only: false,
            }),
        );
        logical.canonicalize();
        coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(self.invocation(sequence), &logical, 0, 0)
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
    // Generous on purpose. Every caller of this helper asserts that an
    // answer arrived, never that one arrived quickly: the timeout is
    // here so a wedged domain fails the test instead of hanging it, and
    // a sustained-load test sharing a machine with the rest of the suite
    // can legitimately take tens of seconds for one request without
    // anything being wrong. A test that needs the whole load to finish
    // inside a bound sets that bound itself.
    let bytes = tokio::time::timeout(Duration::from_secs(45), async {
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

/// A caller binds a session against the running daemon, and the
/// acknowledgement follows the cluster agreeing that the session
/// exists.
///
/// The binding is not the frontend's own answer any more. Verifying the
/// credential says who was authenticated; it does not make a session.
/// So the frontend mints an establishment receipt, submits the
/// `ConsumeAdmission` command the receipt authorizes, and holds this
/// stream until that command has been proposed, executed against
/// current replicated policy and materialized. The acknowledgement the
/// caller reads is written after that, which is why a one-voter cluster
/// is used here: a bind against a cluster that cannot reach quorum is
/// not acknowledged, which the next test shows for a request.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_binds_a_session_against_the_running_daemon() {
    let dir = workspace("bind");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);

    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    assert_eq!(caller.session, coord_types::ids::SessionId([0x44; 16]));

    // Binding again on a second connection converges on the row the
    // first established rather than creating a second session: the
    // receipt this credential carries is already consumed, and the
    // session that exists is the one it describes.
    let again = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    assert_eq!(again.session, caller.session);
}

/// A process that only serves clients starts, binds no peer listener,
/// and takes a caller's binding to the voters.
///
/// A frontend reaches every committed voter over the api plane, as a
/// collector: they are its destinations, not peers it votes with. A
/// process that took "there are voters to reach" to mean "this process
/// has a peer plane" would demand a peer listener a frontend is not
/// required to bind, and so a supported role could never start.
///
/// A binding is a replicated command, so with none of the committed
/// voters running the frontend holds it rather than answering it: that
/// it negotiates and holds the binding is what says it is serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_frontend_only_process_starts_without_a_peer_plane() {
    let dir = workspace("frontend");
    let ca = credentials(&dir, 7, coord_types::wire_v1::PeerRole::Frontend);
    genesis(&dir, Some(&ca.node_spki));
    endpoints(&dir, &ca, 3, &[]);
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    let text = std::fs::read_to_string(&path).expect("read");
    std::fs::write(
        &path,
        text.replace("role = \"voter-frontend-observer\"", "role = \"frontend\"")
            .replace("peer_quic = \"127.0.0.1:0\"\n", ""),
    )
    .expect("write");

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let (_endpoint, connection, _control, _control_recv) = Caller::negotiated(&daemon, &ca).await;
    let token = service_token(&ring, [0x45; 16]);
    let (mut send, mut recv) = connection.open_bi().await.expect("bind stream");
    send.write_all(&coord_session::bind_frame(token.as_bytes()).expect("bind frame"))
        .await
        .expect("written");
    send.finish().expect("finished");
    let mut buf = [0u8; 4096];
    let early = tokio::time::timeout(Duration::from_secs(3), recv.read(&mut buf)).await;
    assert!(
        early.is_err(),
        "a frontend answered a binding no voter agreed to: {early:?}"
    );
    let said = daemon.said();
    assert!(
        !said.contains("peer listener"),
        "a frontend was asked for a peer plane: {said}"
    );
}

/// A submission no voter could take is offered again on a quiet
/// frontend-only domain, without the caller sending anything more
/// (task-c01).
///
/// The collector schedules the re-offer, but nothing in it runs on its
/// own: the domain loop carries it out. A rejected fan-out is recorded
/// before the retry floor has passed, so the pass that recorded it finds
/// nothing due, and a frontend-only domain has no voter turn and no
/// payload timer -- it waited on its sockets, and the submission was
/// offered again only when unrelated traffic arrived or the caller gave
/// up and retried. The loop now wakes when the collector's next re-offer
/// falls due. Here no voter is running, so every offer is refused as
/// unavailable, and the caller sends its binding once and then nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_frontend_offers_a_refused_submission_again_without_a_caller_retry() {
    let dir = workspace("frontend-reoffer");
    let ca = credentials(&dir, 7, coord_types::wire_v1::PeerRole::Frontend);
    genesis(&dir, Some(&ca.node_spki));
    endpoints(&dir, &ca, 3, &[]);
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    let text = std::fs::read_to_string(&path).expect("read");
    std::fs::write(
        &path,
        text.replace("role = \"voter-frontend-observer\"", "role = \"frontend\"")
            .replace("peer_quic = \"127.0.0.1:0\"\n", ""),
    )
    .expect("write");

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let (_endpoint, connection, _control, _control_recv) = Caller::negotiated(&daemon, &ca).await;
    let token = service_token(&ring, [0x46; 16]);
    let (mut send, _recv) = connection.open_bi().await.expect("bind stream");
    send.write_all(&coord_session::bind_frame(token.as_bytes()).expect("bind frame"))
        .await
        .expect("written");
    send.finish().expect("finished");

    // The binding is a replicated command: the frontend submits it to
    // the three committed voters, none of which is there. After that the
    // caller is silent and nothing else reaches this process.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let said = loop {
        let said = daemon.said();
        if said.contains("offered a submission again") {
            break said;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a quiet frontend never offered the refused submission again:\n{said}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // And it goes on doing so on the collector's schedule, still with
    // nothing arriving: the wake is not a one-off.
    let before = said.matches("offered a submission again").count();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let said = daemon.said();
        if said.matches("offered a submission again").count() > before {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the re-offers stopped on a quiet frontend:\n{said}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    // Its *outcome* is now a real one: the binding established this
    // caller's session as a replicated command, so the Put executes
    // against a session row the cluster agreed on.
    let mut logical = coord_types::logical_v1::LogicalRequest::new(
        coord_types::ids::NamespaceId(REQUEST_NAMESPACE),
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
    // here rather than taken on trust: a mutation, with a revision,
    // against a session row this path wrote.
    //
    // Nothing in the test wrote that row. The bind verified the
    // credential, minted an establishment receipt, and submitted a
    // `ConsumeAdmission` command; the voter proposed it, executed it
    // against current replicated policy, and the acknowledgement the
    // caller read was written only after that was durable. The Put
    // afterwards is authorized by the session that command created and
    // by the permission the domain's genesis granted its principal.
    let coord_types::wire_v1::OutcomeV1::Ok { result, revision } = &response.outcome else {
        panic!("the daemon answered with a transport-level error: {response:?}");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(
        executed.outcome,
        coord_state::Outcome::Put { prev: None },
        "the first command of an established session executes"
    );
    assert!(
        revision.is_some(),
        "a mutation that executed produced no revision: {response:?}"
    );
}

/// The same invocation is answered the same way after a restart.
///
/// The daemon stops, the process that held the collector's retained
/// results and every in-memory command goes with it, and the next
/// process comes up on the store the first one left: it recovers, it
/// serves, and the same invocation gets the same answer.
///
/// This is the retry-resolution path, not a re-execution of the same
/// request. The command executed in the first process and left a
/// retained record keyed by this invocation; the second process cannot
/// tell from its empty command table that it has seen the command, so
/// it re-proposes it -- and the applier answers from the record rather
/// than executing anything, at the position the command already has.
/// The caller gets the same command identity and the same result.
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
        .unwrap_or_else(|| panic!("the daemon answered the retry\n{}", daemon.said()));
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

/// A read retried after a restart is answered with its retained result,
/// not refused.
///
/// A read's output is gated on the request's namespace and keys, which
/// the frontend records when its dispatcher accepts a live request. A
/// retry answered from the durable record never reaches the dispatcher,
/// and the process that recorded them is gone -- so without recording
/// them from the retry itself, every read-bearing retry after a restart
/// was answered as not admitted while the session and its permissions
/// were unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_retried_after_a_restart_gets_its_retained_result() {
    let dir = workspace("read-retry");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x46; 16]).await;
        ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .expect("the daemon answered the write");
        let answer = ask(&caller.connection, &caller.range(2, b"k"))
            .await
            .expect("the daemon answered the read");
        response_of(&answer)
    };
    let coord_types::wire_v1::OutcomeV1::Ok { result, .. } = &first.outcome else {
        panic!("the read was not answered with a result: {first:?}");
    };
    let read: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the read's result decodes");
    assert!(
        !matches!(read.outcome, coord_state::Outcome::ErrPermissionDenied),
        "the read itself was denied, so this is not the case under test: {read:?}"
    );

    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x46; 16]).await;
    let answer = ask(&caller.connection, &caller.range(2, b"k"))
        .await
        .unwrap_or_else(|| panic!("the daemon answered the retry\n{}", daemon.said()));
    let again = response_of(&answer);
    assert_eq!(again.command_id, first.command_id);
    assert_eq!(
        again.outcome, first.outcome,
        "a read retried after a restart was not given its retained result"
    );
}

/// A restarted replica comes back owing what it already owed.
///
/// The consensus machine is wired from the durable record -- the
/// commands it holds, their payloads, what it has executed -- and not
/// from an empty table. A replica that came back blank would have
/// forgotten proposals it had voted on, and would be free to vote
/// differently on them.
///
/// The record it reads is the authoritative one. On this profile the
/// journal is the record and the projection may lag it, so a summary
/// built from the projection alone can omit an obligation that is
/// already durable; the persistence seam offers only the authoritative
/// cut, so there is no second source to pick by mistake. That the cut
/// and the projection genuinely differ while materialization lags is
/// `coord-storage`'s own test; what this holds is that the daemon uses
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_replica_recovers_what_it_already_owed() {
    let dir = workspace("recover");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    // A fresh store owes nothing, and says so.
    {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
        ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .expect("the daemon answered the request");
    }

    let report = start_and_report(&path);
    let line = report
        .lines()
        .find(|l| l.starts_with("recovered "))
        .unwrap_or_else(|| panic!("the daemon said nothing about what it recovered:\n{report}"));
    let field = |name: &str| -> u64 {
        line.split_whitespace()
            .find_map(|f| f.strip_prefix(&format!("{name}=")))
            .unwrap_or_else(|| panic!("no {name} in {line:?}"))
            .parse()
            .unwrap_or_else(|_| panic!("{name} is not a number in {line:?}"))
    };
    assert!(
        field("records") >= 1,
        "no command survived the restart: {line}"
    );
    assert!(field("payloads") >= 1, "no payload survived: {line}");
    assert!(
        field("frontier") >= 1,
        "the recovered summary says nothing has executed: {line}"
    );
    assert!(
        field("position") >= 1,
        "the next command would take a position already taken: {line}"
    );
    // Both commands wrote an executed identity: the establishment this
    // caller's binding submitted, and the request it made afterwards.
    // A refusal would take its position without writing one, so this is
    // what says the work was executed rather than merely ordered.
    assert!(
        field("executed") >= 1,
        "nothing that executed survived the restart: {line}"
    );
}

/// A voter that cannot say where its peers are does not come up.
///
/// Addresses are not committed configuration -- the manifest says who
/// the voters are and what key each proves with, and deliberately not
/// where any of them is. They come from a catalog a committed voter
/// attested, and a voter that has peers and no usable catalog would
/// serve requests it could never establish: from outside that looks
/// like a slow cluster rather than a misconfigured node.
///
/// The catalog is an address book and nothing more. It cannot introduce
/// a voter or re-incarnate one, and whoever answers at an address still
/// has to prove from its certificate that it is the voter the committed
/// configuration names.
#[test]
fn a_voter_that_cannot_find_its_peers_refuses_to_start() {
    let dir = workspace("peers");
    // The fixture `config` writes, kept at hand: the node's genesis is
    // pinned at `init`, so its catalog is restored below under the same
    // credentials rather than a re-issued key and a rewritten manifest,
    // which would be another genesis.
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis(&dir, Some(&ca.node_spki));
    endpoints(&dir, &ca, 3, &[]);
    let _ = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let catalog = dir.join("endpoints.bin");

    // Named and missing.
    std::fs::remove_file(&catalog).expect("remove");
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("cannot read the endpoint catalog"),
        "the refusal did not say what was missing: {}",
        refused.err
    );
    assert!(
        !refused.out.contains("listening"),
        "a voter that cannot reach its peers bound a listener anyway: {}",
        refused.out
    );

    // Present and not attested by a voter of this domain.
    let stranger = Ca::new();
    let (certificate, key) = stranger.issue(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
    );
    let _ = certificate;
    let mut theirs = stranger;
    theirs.node_key = key.serialize_der();
    endpoints(&dir, &theirs, 3, &[]);
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("not this domain's committed voters'"),
        "an unattested catalog was accepted: {}",
        refused.err
    );

    // Not named at all, with peers to reach.
    endpoints(&dir, &ca, 3, &[]);
    let text = std::fs::read_to_string(&path).expect("read");
    std::fs::write(
        &path,
        text.lines()
            .filter(|l| !l.starts_with("cluster_endpoints"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .expect("write");
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("names no cluster_endpoints"),
        "a voter with peers started without an address for any of them: {}",
        refused.err
    );

    // And with the catalog back, it starts and says what it found.
    std::fs::write(&path, text).expect("restore");
    let report = start_and_report(&path);
    assert!(
        report.contains("peers reachable=2 of 2"),
        "the daemon did not report the peers it resolved:\n{report}"
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

/// A binding the daemon refuses establishes nothing.
///
/// The credential is well formed and signed -- by a key this cluster
/// does not trust. The daemon refuses the binding and closes the
/// connection, and no `ConsumeAdmission` is submitted: a caller whose
/// credential does not verify cannot cause a session to exist, which a
/// later *valid* binding of the same session shows by creating it.
#[tokio::test(flavor = "multi_thread")]
async fn a_binding_the_daemon_refuses_establishes_nothing() {
    let dir = workspace("refused");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);

    // Another signing key entirely: the same claims, an untrusted
    // signature.
    let stranger = {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
        coord_sts::KeyRing::new(
            coord_sts::SigningKey::from_pkcs8_der("stranger-1", &key.serialize_der())
                .expect("signing key"),
        )
    };
    let (_endpoint, connection, _control, _control_recv) = Caller::negotiated(&daemon, &ca).await;
    let token = service_token(&stranger, [0x44; 16]);
    let (mut send, mut recv) = connection.open_bi().await.expect("bind stream");
    send.write_all(&coord_session::bind_frame(token.as_bytes()).expect("bind frame"))
        .await
        .expect("written");
    send.finish().expect("finished");
    // A refused binding is a statement about the connection, not about
    // this stream: the daemon closes the peer rather than answering.
    // Which of the two the caller sees first is a race between the
    // close and the stream ending -- both are the refusal, and what
    // must never arrive is an acknowledgement.
    let mut buf = [0u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(20), recv.read(&mut buf))
        .await
        .expect("the daemon decided within the bound");
    match read {
        Ok(None) => {}
        Err(quinn::ReadError::ConnectionLost(quinn::ConnectionError::ApplicationClosed(close))) => {
            assert_eq!(close.error_code.into_inner(), 2, "{close:?}")
        }
        other => panic!("the daemon answered a binding it could not verify: {other:?}"),
    }

    // The session the refused credential named does not exist: a valid
    // credential for it is still the one that creates it, and a caller
    // whose first command executes proves the row is there.
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
    let coord_types::wire_v1::OutcomeV1::Ok { result, .. } = &response_of(&answer).outcome else {
        panic!("a transport-level error");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(executed.outcome, coord_state::Outcome::Put { prev: None });
}

/// One voter in this process is one voter, not a quorum.
///
/// The same code, the same local route -- and three committed voters
/// instead of one. The caller presents a credential this node verifies
/// perfectly well, so the frontend mints its establishment receipt and
/// submits the command; the frame reaches this node's voter without a
/// network hop, the voter proposes it, and the collector counts exactly
/// one contribution: its own. Nothing is released, because nothing has
/// agreed, and the binding is not acknowledged.
///
/// That the *binding* is what hangs here is the point of task-j09: a
/// verified credential says who was authenticated, and the session it
/// names exists only once the cluster has agreed to create it. A
/// frontend that acknowledged the binding on its own verification, or a
/// co-located voter that pre-counted itself, would answer this caller.
#[tokio::test(flavor = "multi_thread")]
async fn one_co_located_voter_is_not_a_quorum() {
    let dir = workspace("noquorum");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 3, Some(&ca.node_spki));
    endpoints(&dir, &ca, 3, &[]);
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let (_endpoint, connection, _control, _control_recv) = Caller::negotiated(&daemon, &ca).await;

    let token = service_token(&ring, [0x44; 16]);
    let (mut send, mut recv) = connection.open_bi().await.expect("bind stream");
    send.write_all(&coord_session::bind_frame(token.as_bytes()).expect("bind frame"))
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
        "one voter established a session for a quorum of three: {early:?}"
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

/// A free loopback UDP port.
///
/// Bound, read and released: a test has to know the address before the
/// daemon exists, because the endpoint catalog names it and a voter
/// reads that before it binds anything. The window between here and the
/// daemon's own bind is the standard cost of naming a port in advance.
///
/// The port is chosen below the kernel's ephemeral range rather than
/// taken from it. Every test in this file runs in its own process, and
/// the ones that bind `:0` -- a client endpoint, a daemon told to pick
/// its own address -- are handed ephemeral ports; asking the kernel for
/// one here too meant a port released by this test could be the next
/// one the kernel gave a neighbour, and the daemon then found its named
/// address taken.
///
/// Below that range, each process owns a slice of 100 ports outright.
/// It holds an exclusive lock on a file named for the slice for as long
/// as it runs, so no other process of this suite alive at the same time
/// draws from it. Keying the slice by process id alone was not enough: a
/// restart test releases a voter's port while the voter is down, and a
/// process whose id fell in the same slice could bind that port in the
/// meantime, so the restarted voter found its named address taken
/// (`Address already in use`). The kernel drops the lock when the
/// process ends, however it ends. The probe below still catches a port
/// something outside this suite holds, and a slice that runs dry takes
/// another one.
fn free_port() -> u16 {
    use std::sync::Mutex;
    const FIRST: u32 = 10000;
    const SLICE: u32 = 100;
    // 227 slices of 100 end at 32700, still below the ephemeral range.
    const SLICES: u32 = 227;
    // The slice being drawn from, the next port in it, and the lock
    // files of every slice this process has claimed.
    static OWNED: Mutex<(Option<u32>, u32, Vec<std::fs::File>)> = Mutex::new((None, 0, Vec::new()));

    let claim = |held: &mut Vec<std::fs::File>, after: u32| -> u32 {
        let dir = std::env::temp_dir().join("coordd-cli-ports");
        std::fs::create_dir_all(&dir).expect("a directory for the port locks");
        for step in 0..SLICES {
            let slice = (after + step) % SLICES;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join(format!("slice-{slice}")))
                .expect("a port lock file");
            if file.try_lock().is_ok() {
                held.push(file);
                return slice;
            }
        }
        panic!("every port slice in 10000..32700 is held by another test process");
    };

    let mut owned = OWNED.lock().expect("not poisoned");
    let (slice, drawn, held) = &mut *owned;
    for _ in 0..SLICES * SLICE {
        let current = match *slice {
            Some(current) if *drawn < SLICE => current,
            previous => {
                let start = previous.map_or(std::process::id() % SLICES, |p| p + 1);
                let claimed = claim(held, start);
                *slice = Some(claimed);
                *drawn = 0;
                claimed
            }
        };
        let port = u16::try_from(FIRST + current * SLICE + *drawn).expect("below 32768");
        *drawn += 1;
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free loopback port in 10000..32700");
}

/// Three real daemons of one domain: one certificate authority, three
/// node identities the genesis commits, one signed catalog naming where
/// each of them listens.
struct Cluster {
    ca: Ca,
    ring: coord_sts::KeyRing,
    configs: Vec<PathBuf>,
    api: Vec<u16>,
}

fn three_voters(dir: &Path) -> Cluster {
    let ca = Ca::new();
    let mut ca = ca;
    let mut spki = Vec::new();
    let mut keys = Vec::new();
    let api: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let peer: Vec<u16> = (0..3).map(|_| free_port()).collect();

    let ring = {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("sts key");
        coord_sts::KeyRing::new(
            coord_sts::SigningKey::from_pkcs8_der("coordd-test-1", &key.serialize_der())
                .expect("signing key"),
        )
    };

    for n in 1u8..=3 {
        let node = dir.join(format!("n{n}"));
        std::fs::create_dir_all(&node).expect("node dir");
        let (certificate, key) = ca.issue(
            SERVER_NAME,
            CLUSTER,
            n,
            coord_types::wire_v1::PeerRole::Voter,
        );
        spki.push(spki_of(certificate.der()));
        keys.push(key.serialize_der());
        std::fs::write(node.join("node.pem"), pem("CERTIFICATE", certificate.der())).expect("cert");
        std::fs::write(node.join("roots.pem"), ca.root_pem()).expect("roots");
        let key_path = node.join("node.key");
        std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                .expect("chmod");
        }
        collector_credential(&node, &ca, n);
        std::fs::write(
            node.join("sts-jwks.json"),
            serde_json::to_vec_pretty(&ring.jwks()).expect("jwks"),
        )
        .expect("write jwks");
    }

    // Genesis commits all three keys, so each of them is a voter the
    // others will accept and none of them can be impersonated.
    let voters: Vec<serde_json::Value> = (1u8..=3)
        .map(|n| {
            serde_json::json!({
                "node": hex(&[n; 16]),
                "incarnation": 1,
                "public_key": b64url(&spki[usize::from(n) - 1]),
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

    // One catalog, attested by voter 1, naming every voter's peer
    // listener. The same file for all three: an address book is not
    // per-node state.
    ca.node_key = keys[0].clone();
    endpoints(
        dir,
        &ca,
        3,
        &(1u8..=3)
            .map(|n| {
                let i = usize::from(n) - 1;
                // Both of this node's listeners, in one entry. A voter
                // dialling the peer plane and a collector dialling the
                // api plane offer different ALPNs, so each one's wrong
                // address fails to negotiate and the other is tried.
                (
                    n,
                    vec![
                        format!("127.0.0.1:{}", peer[i]),
                        format!("127.0.0.1:{}", api[i]),
                    ],
                )
            })
            .collect::<Vec<_>>(),
    );

    let configs = (1u8..=3)
        .map(|n| {
            let node = dir.join(format!("n{n}"));
            let i = usize::from(n) - 1;
            let text = format!(
                r#"config_version = 2
role = "voter-frontend-observer"
cluster_manifest = "{root}/genesis.json"
cluster_endpoints = "{root}/endpoints.bin"
domain = "control-plane-test"
state_directory = "{node}"

[listen]
api_quic = "127.0.0.1:{api}"
peer_quic = "127.0.0.1:{peer}"

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
trust_bundle = "{node}/roots.pem"
node_certificate = "{node}/node.pem"
node_key = "{node}/node.key"
collector_certificate = "{node}/collector.pem"
collector_key = "{node}/collector.key"

[sts]
issuer = "https://sts.test"
resource = "control-plane-test"
jwks = "{node}/sts-jwks.json"
trust_rule = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"

[[grant]]
principal = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"
namespace = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e"
"#,
                root = dir.display(),
                node = node.display(),
                api = api[i],
                peer = peer[i],
            );
            let path = node.join("coordd.toml");
            std::fs::write(&path, text).expect("write config");
            path
        })
        .collect();

    Cluster {
        ca,
        ring,
        configs,
        api,
    }
}

/// A request is established by three separate processes agreeing.
///
/// This is the three-voter served-request gate, and it is a different
/// claim from the single-voter one. There, the quorum was the one voter
/// in the process that received the request, so every stage happened in
/// one address space. Here the committed quorum is two of three, and
/// the process the caller reached holds one vote: an answer comes back
/// only if this node's frontend submitted to the other two as this
/// domain's *collector* -- over the api plane, presenting the collector
/// credential, because a node certificate binds one role and the role
/// that may submit on a client's behalf is not the voter's -- and only
/// if those voters' evidence came back to the collector that submitted
/// rather than to the one sharing each of their processes, and met the
/// committed quorum rule under their own committed identities.
///
/// None of that is a shortcut for a co-located voter. The frontend's
/// own voter is reached through its bounded ingress instead of a
/// socket, and contributes exactly one voter's evidence through the
/// same collector validation as the other two.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_is_established_by_three_voters_agreeing() {
    let dir = workspace("quorum");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0), "each store is made");
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "voter {} cannot submit to the other two:\n{}",
            n + 1,
            node.said()
        );
    }

    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x44; 16]).await;
    let Some(answer) = ask(&caller.connection, &caller.put(1, b"k", b"v")).await else {
        panic!(
            "three voters never established the request\n-- voter 1 --\n{}\n             -- voter 2 --\n{}\n-- voter 3 --\n{}",
            running[0].said(),
            running[1].said(),
            running[2].said()
        );
    };
    let response = response_of(&answer);

    // The answer is about the command the caller asked for.
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
        "three voters answered with some other command's result"
    );

    // And it is the replicated execution's own result, decoded here.
    // Two commands were agreed by these three processes, not one: the
    // establishment this caller's binding submitted, and then the Put
    // the session it created authorizes.
    let coord_types::wire_v1::OutcomeV1::Ok { result, revision } = &response.outcome else {
        panic!("the cluster answered with a transport-level error: {response:?}");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(
        executed.outcome,
        coord_state::Outcome::Put { prev: None },
        "three voters executed the first command of an established session"
    );
    assert!(
        revision.is_some(),
        "a mutation with no revision: {response:?}"
    );
}

/// Three real daemons of one domain form a mesh on committed
/// identities, on both of their planes.
///
/// Three processes, three certificates the genesis commits, three
/// stores, and real QUIC between them. Each reads the same signed
/// catalog, which names both of every node's addresses without saying
/// which is which, and ends up holding two kinds of link to each of the
/// other two: a peer link it votes over, and an api link its own
/// collector submits over. Each is established only if the certificate
/// on the far end binds to a committed voter of this domain at its
/// committed incarnation -- and, for the api link, only if the dialling
/// side presented a collector credential rather than its voter one.
///
/// A caller then binds against one of them, so the api plane is serving
/// callers while it is also submitting to peers.
#[tokio::test(flavor = "multi_thread")]
async fn three_voters_form_a_mesh_on_committed_identities() {
    let dir = workspace("three");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(
            run(config, &["init"]).code,
            Some(0),
            "each voter creates its own store"
        );
    }

    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    assert_eq!(running[0].api.port(), cluster.api[0]);

    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_to_say("peers connected=2 of 2"),
            "voter {} does not hold a peer link to both of its peers:\n{}",
            n + 1,
            node.said()
        );
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "voter {} cannot submit to both of its peers:\n{}",
            n + 1,
            node.said()
        );
        // A dial that found nothing listening yet, or found the node's
        // other plane, is ordinary: an address is a hint, the two planes
        // negotiate different ALPNs, and in a full mesh the peer's own
        // dial establishes the link either way. What must not appear is
        // an *identity* refusal -- a certificate this domain's committed
        // configuration does not accept, on either end.
        assert!(
            !node.said().contains("Rejected(Rejected("),
            "voter {} refused a peer's identity or had its own refused:\n{}",
            n + 1,
            node.said()
        );
    }

    // And the api plane is serving while the peer plane is up.
    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x44; 16]).await;
    assert_eq!(caller.session, coord_types::ids::SessionId([0x44; 16]));
}

/// Start `coordd` and report whether it came up, stopping it if it did.
///
/// A daemon that starts holds its listeners and parks, so "it came up"
/// is its own report of the phase it reached, read as it is printed; a
/// daemon that refuses exits, and that is reported as a refusal.
fn comes_up(config: &Path) -> Result<String, Run> {
    use std::io::BufRead;

    let mut child = Command::new(binary())
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("coordd started");
    let stdout = child.stdout.take().expect("stdout");
    let (lines, seen) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut out = String::new();
    loop {
        match seen.recv_timeout(Duration::from_millis(20)) {
            Ok(line) => {
                out.push_str(&line);
                out.push('\n');
                if line.starts_with("coordd phase=") {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Ok(out);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                let output = child.wait_with_output().expect("output");
                return Err(Run {
                    code: output.status.code(),
                    out,
                    err: String::from_utf8_lossy(&output.stderr).into_owned(),
                });
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("coordd neither came up nor stopped within 30s: {out}");
        }
    }
}

/// The genesis a node was initialized under is the only one it serves
/// under.
///
/// The manifest is a file, re-read on every start, and a file can be
/// edited. A node that re-adopted whatever it said would vote by the
/// operator's latest edit -- another set of voters, another policy,
/// under the same cluster and domain -- rather than by the configuration
/// every replica agreed on. So `init` pins it, and a start handed any
/// other manifest is a genesis quarantine, refused before the journal
/// replays anything into the projection.
#[test]
fn a_node_serves_only_under_the_genesis_it_was_initialized_with() {
    let dir = workspace("pinned");
    let path = config(&dir);
    let init = run(&path, &["init"]);
    assert_eq!(init.code, Some(0), "{}{}", init.out, init.err);

    // Under the manifest it was initialized with, it starts.
    if let Err(refused) = comes_up(&path) {
        panic!(
            "the node refused its own genesis: {}{}",
            refused.out, refused.err
        );
    }

    // The same cluster and domain, and this node still a voter under the
    // key it holds; the third voter swapped for a stranger.
    let manifest = std::fs::read_to_string(dir.join("genesis.json")).expect("read");
    std::fs::write(
        dir.join("genesis.json"),
        manifest.replace(&hex(&[3; 16]), &hex(&[4; 16])),
    )
    .expect("swap a voter");
    let refused =
        comes_up(&path).expect_err("a node started under a genesis it was not initialized with");
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("genesis quarantine"),
        "the refusal did not say why: {}",
        refused.err
    );

    // Initializing again is not a way round it: the store is this node's,
    // under the genesis it was pinned to.
    let again = run(&path, &["init"]);
    assert_eq!(again.code, Some(2), "{}{}", again.out, again.err);
    assert!(again.err.contains("already exists"), "{}", again.err);

    // And the original manifest is still this node's.
    std::fs::write(dir.join("genesis.json"), manifest).expect("restore");
    if let Err(refused) = comes_up(&path) {
        panic!(
            "the node refused its restored genesis: {}{}",
            refused.out, refused.err
        );
    }
}

/// An initialization that stopped after it created the store and before
/// it pinned the genesis is refused by a start -- which would otherwise
/// pin whatever it was handed -- and finished by `init`, since a store
/// that was never pinned was never served either.
///
/// The journal it left is reused by that `init`, as it is for one that
/// stopped before the projection existed: the two ways of finishing an
/// initialization compose rather than one refusing the other's store.
#[test]
fn an_unfinished_initialization_is_finished_by_init_and_refused_by_a_start() {
    use coord_store_api::engine::{LocalEngine, WriteTxn};
    use coord_store_api::registry::{Collection, meta_fields};

    let dir = workspace("unpinned");
    let path = config(&dir);
    let init = run(&path, &["init"]);
    assert_eq!(init.code, Some(0), "{}{}", init.out, init.err);

    // The state an initialization interrupted between the two leaves.
    {
        let mut generation = coord_storage_redb::lifecycle::Generation::open_existing(
            &dir.join("state"),
            coord_storage_redb::lifecycle::StoreIdentity {
                cluster_id: coord_types::ids::ClusterId(CLUSTER),
                domain_id: coord_types::ids::DomainId(DOMAIN),
                replica_id: coord_types::ids::ReplicaId([1; 16]),
                incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
            },
            coord_storage_redb::lifecycle::OpenOptions::default(),
        )
        .expect("the store opens");
        let mut txn = generation.engine().begin_write().expect("write");
        txn.delete(Collection::MetaV1.id(), meta_fields::GENESIS_DIGEST)
            .expect("unpin");
        txn.commit_durable().expect("commit");
    }

    let refused = comes_up(&path).expect_err("an unpinned store was served");
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("never pinned") && refused.err.contains("coordd init"),
        "the refusal did not say what to do: {}",
        refused.err
    );

    let finished = run(&path, &["init"]);
    assert_eq!(finished.code, Some(0), "{}{}", finished.out, finished.err);
    assert!(
        !dir.join("state").join("gen-000002").exists(),
        "finishing an initialization made a second generation"
    );
    // The policy was written before the pin, so finishing finds it there
    // and writes none of it again.
    assert!(
        finished.out.contains("genesis policy rows=0 present=")
            && !finished.out.contains("present=0"),
        "finishing rewrote a genesis policy the store already held: {}",
        finished.out
    );
    let report = start_and_report(&path);
    assert!(report.contains("owed=0"), "{report}");

    // Finished is finished: a further `init` is refused as before.
    let again = run(&path, &["init"]);
    assert_eq!(again.code, Some(2), "{}{}", again.out, again.err);
    assert!(again.err.contains("already exists"), "{}", again.err);
}

/// An initialization that stopped before it wrote the genesis policy is
/// finished by `init`, which writes the policy and only then pins, and
/// the finished node serves a caller that policy admits.
///
/// The pin is the last durable step of `init`. Pinned before the policy,
/// a stop between the two left a store every check took for initialized
/// -- a start served it, trusting nothing and granting nothing, and
/// `init` refused it as already initialized -- so a node could never be
/// given the policy it was meant to have.
///
/// The state such a stop leaves is this node's journal and its first
/// projection generation, created under its identity exactly as `init`
/// creates them, with nothing written into either: no policy rows and no
/// pin. It is made here directly, with the same calls.
#[tokio::test(flavor = "multi_thread")]
async fn an_initialization_that_stopped_before_its_policy_is_finished_and_then_serves() {
    let dir = workspace("unpolicied");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    drop(
        coord_journal_raft_engine::journal::RaftEngineJournal::create(
            &dir.join("journal"),
            coord_journal_raft_engine::journal::JournalIdentity {
                cluster: coord_types::ids::ClusterId(CLUSTER),
                replica: coord_types::ids::ReplicaId([1; 16]),
            },
            &coord_journal_raft_engine::journal::JournalOptions::default(),
        )
        .expect("the journal init creates"),
    );
    drop(
        coord_storage_redb::lifecycle::Generation::create(
            &dir.join("state"),
            coord_storage_redb::lifecycle::StoreIdentity {
                cluster_id: coord_types::ids::ClusterId(CLUSTER),
                domain_id: coord_types::ids::DomainId(DOMAIN),
                replica_id: coord_types::ids::ReplicaId([1; 16]),
                incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
            },
            coord_storage_redb::lifecycle::OpenOptions::default(),
        )
        .expect("the generation init creates"),
    );

    // A start refuses it: serving would serve a domain with no policy.
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("never pinned") && refused.err.contains("coordd init"),
        "the refusal did not say what to do: {}",
        refused.err
    );

    // `init` finishes it: the policy it never wrote, and then the pin.
    let finished = run(&path, &["init"]);
    assert_eq!(finished.code, Some(0), "{}{}", finished.out, finished.err);
    assert!(
        finished.out.contains("genesis policy rows=")
            && finished.out.contains(" present=0")
            && !finished.out.contains("rows=0"),
        "finishing did not write the missing policy: {}",
        finished.out
    );
    assert!(
        !dir.join("state").join("gen-000002").exists(),
        "finishing an initialization made a second generation"
    );

    // And the finished node serves: the trust rule admits the caller's
    // binding and the grant authorizes its Put.
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the finished node never answered\n{}", daemon.said()));
    let response = response_of(&answer);
    let coord_types::wire_v1::OutcomeV1::Ok { result, .. } = &response.outcome else {
        panic!("the finished node refused a caller its policy admits: {response:?}");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(executed.outcome, coord_state::Outcome::Put { prev: None });

    // Finished is finished.
    drop(daemon);
    let again = run(&path, &["init"]);
    assert_eq!(again.code, Some(2), "{}{}", again.out, again.err);
    assert!(again.err.contains("already exists"), "{}", again.err);
}

/// A certificate is an identity only if the trust bundle's authority
/// issued it. The replica a node is comes out of its certificate, so a
/// leaf anybody could sign would let anybody open -- or initialize --
/// a voter's store under that voter's name.
#[test]
fn a_certificate_the_trust_bundle_did_not_issue_is_not_an_identity() {
    let dir = workspace("untrusted");
    let path = config(&dir);

    // A leaf naming a committed voter, signed by an authority the bundle
    // does not hold. The manifest commits to its key, so nothing but the
    // issuer is wrong with it.
    let stranger = Ca::new();
    let (certificate, key) = stranger.issue(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
    );
    genesis(&dir, Some(&spki_of(certificate.der())));
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", certificate.der())).expect("cert");
    let key_path = dir.join("node.key");
    let _ = std::fs::remove_file(&key_path);
    std::fs::write(&key_path, pem("PRIVATE KEY", &key.serialize_der())).expect("key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }

    let checked = run(&path, &["--check"]);
    assert_eq!(checked.code, Some(2), "{}{}", checked.out, checked.err);
    assert!(
        checked.err.contains("not issued by the trust bundle"),
        "the refusal did not say why: {}",
        checked.err
    );
    let refused = run(&path, &["init"]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        !dir.join("state").exists() && !dir.join("journal").exists(),
        "an untrusted certificate initialized a voter's durable storage"
    );
}

/// A caller assembled from the Rust pieces a real client has: the
/// `coord-transport` endpoint for the wire, and the `coord-sdk` client
/// for the request lifecycle.
///
/// The direct-quinn `Caller` above exists because the transport had no
/// caller's shape -- `Transport::send` opens a stream and drops the half
/// the answer comes back on. `Transport::request` is that shape, and
/// this is what says so: the SDK decides what to send and what an answer
/// means, the transport carries it, and neither of them is a test
/// double.
struct SdkCaller {
    transport: coord_transport::Transport,
    connection: coord_transport::ConnectionId,
    client: coord_sdk::Client<coord_sdk::StaticProvider>,
    session: coord_types::ids::SessionId,
}

impl SdkCaller {
    /// Dial `daemon` and bind `session`, driving the SDK's own binding
    /// action rather than writing a bind frame directly.
    async fn bind(
        dir: &Path,
        daemon: &Running,
        ca: &Ca,
        ring: &coord_sts::KeyRing,
        session: [u8; 16],
    ) -> Self {
        use coord_types::wire_v1::PeerRole;

        let (certificate, key) = ca.issue("caller.coordd.test", CLUSTER, 0x0c, PeerRole::Client);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls_pki_types::CertificateDer::from(
                ca.certificate.der().to_vec(),
            ))
            .expect("ca root");
        // The caller's own binder is the committed membership's, from
        // the same manifest the daemon read: what it is dialing is a
        // voter of this domain because the configuration says the
        // certificate it presented is, or it is nobody.
        let manifest: coord_membership::genesis::GenesisManifest = serde_json::from_slice(
            &std::fs::read(dir.join("genesis.json")).expect("the manifest is readable"),
        )
        .expect("a manifest");
        let membership =
            coord_membership::membership::Membership::from_genesis(&manifest).expect("membership");
        let identity = coord_transport::LocalIdentity {
            cluster: coord_types::ids::ClusterId(CLUSTER),
            domain: coord_types::ids::DomainId(DOMAIN),
            chain: vec![rustls_pki_types::CertificateDer::from(
                certificate.der().to_vec(),
            )],
            key: rustls_pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            roots: std::sync::Arc::new(roots),
            capabilities: coord_transport::role_lanes(PeerRole::Client)
                .iter()
                .map(|lane| lane.capability())
                .collect(),
            api_client: None,
            serves: Some(coord_transport::Class::Api),
            replica: None,
        };
        let transport = coord_transport::Transport::bind(
            "127.0.0.1:0".parse().expect("loopback"),
            identity,
            std::sync::Arc::new(coord_membership::binder::PeerBinder::new(membership)),
            coord_transport::Limits::default(),
        )
        .expect("the caller's endpoint");
        let connection = transport
            .connect(
                daemon.api,
                SERVER_NAME,
                PeerRole::Client,
                None,
                coord_transport::Lane::Unary,
                coord_transport::BoundIdentity {
                    role: PeerRole::Voter,
                    replica: Some(coord_types::ids::ReplicaId([1; 16])),
                    incarnation: Some(coord_types::ids::ReplicaIncarnation::new(1).expect("one")),
                    capabilities: Vec::new(),
                },
            )
            .await
            .expect("the caller reached the daemon");

        // The credential the SDK presents is the service token the
        // daemon's own keys verify. Expiry is in the SDK's ticks
        // (milliseconds), which is also the clock the client is driven
        // on below.
        let token = service_token(ring, session);
        let mut client = coord_sdk::Client::new(
            coord_sdk::ClientConfig::default(),
            coord_sdk::ClientInstance::new(
                coord_types::ids::ClusterId(CLUSTER),
                coord_types::ids::DomainId(DOMAIN),
                coord_types::ids::SessionId(session),
                coord_types::ids::ClientInstanceId([0x0c; 16]),
            ),
            coord_sdk::StaticProvider::new(coord_sdk::Credential::new(
                token.into_bytes(),
                u64::MAX,
            )),
        );
        let sdk_connection = coord_sdk::ConnectionId(connection.0);
        client
            .connect(0, sdk_connection)
            .expect("the client opened its connection");

        // Exactly one action, and it is the binding: a credential is
        // presented once per connection and never again.
        let actions = client.take_actions();
        let [coord_sdk::SdkAction::Bind { credential, .. }] = actions.as_slice() else {
            panic!("the client did not ask to bind: {actions:?}");
        };
        let frame = coord_session::bind_frame(credential.present()).expect("bind frame");
        let answer = transport
            .request(connection, frame, Duration::from_secs(20))
            .await
            .expect("the daemon answered the binding");
        let ack = coord_session::decode_bind_ack(&answer).expect("a binding acknowledgement");
        assert_eq!(ack.session, coord_types::ids::SessionId(session));
        client.bound(0, sdk_connection);

        SdkCaller {
            transport,
            connection,
            client,
            session: ack.session,
        }
    }

    /// Submit `request` and carry it to a completion.
    async fn ask(
        &mut self,
        now: u64,
        request: &coord_types::logical_v1::LogicalRequest,
    ) -> coord_sdk::Completion {
        let id = self.client.submit(now, request, 0).expect("submitted");
        let actions = self.client.take_actions();
        let [coord_sdk::SdkAction::Send { frame, .. }] = actions.as_slice() else {
            panic!("the client did not ask to send: {actions:?}");
        };
        let answer = self
            .transport
            .request(self.connection, frame.clone(), Duration::from_secs(20))
            .await
            .expect("the daemon answered the request");
        // Back to the SDK as it arrived, framed: what a frame means is
        // the SDK's to decide, and a caller that decoded it here would
        // be a second opinion about what an answer is.
        let bytes =
            coord_types::wire_v1::encode_frame(answer.kind, answer.version, &answer.payload)
                .expect("re-framed");
        self.client
            .on_frame(now, coord_sdk::ConnectionId(self.connection.0), &bytes)
            .expect("the client accepted the answer");
        let completions = self.client.take_completions();
        let [completion] = completions.as_slice() else {
            panic!("the client did not complete the request: {completions:?}");
        };
        assert_eq!(completion.request, id);
        completion.clone()
    }
}

/// A Rust caller sends a request and reads its answer through the SDK.
///
/// The same daemon the direct-quinn caller drives, and the same request,
/// carried by the pieces a Rust client actually has. The SDK allocates
/// the invocation, builds the frame and decides what the answer means;
/// `Transport::request` carries it and reads the reply on the stream it
/// was asked on.
///
/// What the answer *is* is not this test's claim -- it is the fail-closed
/// refusal every command gets until the session row exists (task-j09).
/// What is claimed is that a Rust caller asked and heard back, through
/// the SDK, with the command identity the SDK allocated.
#[tokio::test(flavor = "multi_thread")]
async fn a_rust_caller_asks_through_the_sdk_and_reads_the_answer() {
    let dir = workspace("sdk");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);

    let mut caller = SdkCaller::bind(&dir, &daemon, &ca, &ring, [0x44; 16]).await;
    assert_eq!(caller.session, coord_types::ids::SessionId([0x44; 16]));

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
    let completion = caller.ask(1, &logical).await;

    // The SDK's own command identity, so the answer it accepted is the
    // one it asked for and not merely the next frame on the wire.
    let coord_sdk::Outcome::Established { result, .. } = &completion.outcome else {
        panic!("the daemon did not establish the request: {completion:?}");
    };
    let executed: coord_state::Response =
        postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
    assert_eq!(
        executed.outcome,
        coord_state::Outcome::Put { prev: None },
        "the SDK's caller executed against the session its binding established"
    );
}

/// A caller that keeps asking is still answered.
///
/// The number is not arbitrary: the leader's command table is created
/// with a capacity, and a table that never forgets an executed command
/// would make that capacity a *lifetime* bound rather than a bound on
/// outstanding work. A voter that answered its first sixty-four callers
/// and then silently stopped would pass every test this project had,
/// because nothing before this one ever asked it a hundred questions in
/// a row. A Kubernetes API server asks it that many while it is still
/// booting.
///
/// Sequential on purpose: one request is in flight at a time, so
/// nothing here is a concurrency bound being reached. Each request is a
/// distinct key, so nothing is deduplicated either.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_keeps_asking_is_still_answered() {
    let dir = workspace("sustained");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let mut caller = SdkCaller::bind(&dir, &daemon, &ca, &ring, [0x44; 16]).await;

    // Comfortably past the table's capacity, which is 64.
    const ASKS: usize = 200;
    for n in 0..ASKS {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            coord_types::ids::NamespaceId([0x5e; 16]),
            coord_types::logical_v1::CanonicalOperation::Put(coord_types::logical_v1::PutOp {
                key: format!("k{n:04}").into_bytes(),
                value: b"v".to_vec(),
                lease: None,
                prev_kv: false,
            }),
        );
        logical.canonicalize();
        let completion = caller.ask(n as u64 + 1, &logical).await;
        let coord_sdk::Outcome::Established { .. } = &completion.outcome else {
            panic!(
                "request {n} of {ASKS} was not established: {:?}\n{}",
                completion.outcome,
                daemon.said()
            );
        };
    }
}

/// A deadline that passes makes the outcome unknown, not failed, and the
/// invocation stays resolvable by its identity.
///
/// The request really is sent and really is answered; what does not
/// happen is the answer reaching this caller. That is the ambiguous case
/// the whole identity scheme exists for: the client cannot tell whether
/// the command happened, so it says unknown, keeps the invocation, and
/// asks again by identity rather than sending a second command.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_this_caller_never_saw_is_unknown_and_still_resolvable() {
    let dir = workspace("sdk-unknown");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let mut caller = SdkCaller::bind(&dir, &daemon, &ca, &ring, [0x44; 16]).await;

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

    // Sent and answered, and the answer dropped on the floor here.
    let id = caller.client.submit(1, &logical, 50).expect("submitted");
    let actions = caller.client.take_actions();
    let [coord_sdk::SdkAction::Send { frame, .. }] = actions.as_slice() else {
        panic!("{actions:?}");
    };
    let established = caller
        .transport
        .request(caller.connection, frame.clone(), Duration::from_secs(20))
        .await
        .expect("the daemon answered");

    // The deadline passes with nothing fed back.
    caller.client.tick(200);
    let completions = caller.client.take_completions();
    assert_eq!(
        completions
            .iter()
            .map(|c| c.outcome.clone())
            .collect::<Vec<_>>(),
        vec![coord_sdk::Outcome::Unknown],
        "a deadline is an unknown outcome, never a failed one"
    );

    // Two actions, and both matter. The abandoned stream is reset
    // explicitly -- nothing else closes it, and the resolution needs its
    // credit on a connection sized for one -- and the invocation is
    // asked about by its identity rather than sent again.
    let actions = caller.client.take_actions();
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, coord_sdk::SdkAction::Reset { .. })),
        "the abandoned stream was not reset: {actions:?}"
    );
    let Some(coord_sdk::SdkAction::Resolve { frame, .. }) = actions
        .iter()
        .find(|a| matches!(a, coord_sdk::SdkAction::Resolve { .. }))
    else {
        panic!("the client did not ask to resolve: {actions:?}");
    };
    let resolved = caller
        .transport
        .request(caller.connection, frame.clone(), Duration::from_secs(20))
        .await
        .expect("the daemon answered the resolution");

    assert_eq!(
        response_of_frame(&resolved).command_id,
        response_of_frame(&established).command_id,
        "the resolution named another command"
    );
    let bytes =
        coord_types::wire_v1::encode_frame(resolved.kind, resolved.version, &resolved.payload)
            .expect("re-framed");
    caller
        .client
        .on_frame(400, coord_sdk::ConnectionId(caller.connection.0), &bytes)
        .expect("the client accepted the resolution");
    let completions = caller.client.take_completions();
    let [completion] = completions.as_slice() else {
        panic!("the resolution did not complete the request: {completions:?}");
    };
    assert_eq!(completion.request, id);
    let coord_sdk::Outcome::Established { result, .. } = &completion.outcome else {
        panic!("the resolution did not return a result: {completion:?}");
    };
    let coord_types::wire_v1::OutcomeV1::Ok { result: first, .. } =
        response_of_frame(&established).outcome
    else {
        panic!("the daemon's first answer was not a result");
    };
    assert_eq!(
        result.as_slice(),
        first.as_slice(),
        "resolving the same invocation gave a different answer than the request did"
    );

    // Cancelling releases this caller's slot. It says the outcome is
    // known now, and it does not say anything about the command being
    // undone: nothing here could, because the record is the cluster's.
    assert_eq!(caller.client.outstanding(), 0, "the request is finished");
    assert!(caller.client.forget(id).is_some());
}

/// The response a frame carries.
fn response_of_frame(frame: &coord_types::wire_v1::Frame) -> coord_types::wire_v1::ResponseV1 {
    match coord_types::wire_v1::decode(frame) {
        Ok(coord_types::wire_v1::MessageV1::Response(r)) => r,
        other => panic!("not a response: {other:?}"),
    }
}

/// A running node publishes its own recovery baseline, reclaims the
/// journal prefix it represents, and comes back on it.
///
/// This is task-j04's composition on the serving path rather than in a
/// harness: the image is of the projection this daemon actually
/// materialized, the pointer goes into the journal this daemon actually
/// writes, and the prefix that is retired is the one it no longer needs.
/// Every piece is checked separately in `coord-checkpoint`; what this
/// holds is that a running daemon does it at all, and that having done
/// it the node still serves.
///
/// The last part is the one worth having. Retiring a prefix is the only
/// operation in the system that deliberately destroys durable history,
/// so the question a test has to answer is not "did it publish" but
/// "does the node still know what it promised afterwards" -- and the
/// evidence for that is the same invocation being answered the same
/// way by a process that started from the image plus what the journal
/// still holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_publishes_its_own_baseline_and_comes_back_on_it() {
    let dir = workspace("baseline");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    checkpoint_after(&path, 1);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
        let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
        // The cycle ran, and it ran all the way: the pointer is durable
        // and the prefix it represents is gone from the journal. A
        // publication that could not retire would say `retired=false`,
        // which is a legitimate outcome of the operation and not of
        // this one -- nothing here fails the compaction.
        assert!(
            daemon.waits_to_say("retired=true"),
            "the daemon never published a recovery checkpoint\n{}",
            daemon.said()
        );
        response_of(&answer)
    };

    // Images do not accumulate. Step 5 of the publication order
    // reclaims what a newer baseline supersedes, and a node that
    // published without reclaiming would fill its disk with the history
    // it had just decided it did not need.
    //
    // Two, not one, because stopping the daemon can land between the
    // pointer becoming durable and the cleanup finishing -- the
    // "trim complete, old cleanup interrupted" row of the crash matrix,
    // where the selected image plus the suffix suffice and the leftover
    // is garbage the next publication collects. What must never happen
    // is one image per publication, and this daemon published many.
    //
    // Images only. A `.pending-` directory is a write that was
    // interrupted, and it is not an image: nothing selects it, and the
    // next publication reclaims it along with the superseded images.
    let images: Vec<_> = std::fs::read_dir(dir.join("checkpoints"))
        .expect("the daemon created its checkpoint directory")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|name| !name.starts_with(".pending-"))
        .collect();
    assert!(
        (1..=2).contains(&images.len()),
        "the checkpoint directory holds {} images: {images:?}",
        images.len()
    );

    // A different process, on a journal whose prefix has been retired.
    // It says which baseline it recovers from, and it says it before it
    // serves -- an image this node published and can no longer load is
    // the loss of a durable prefix, and the startup line is where that
    // has to surface.
    let report = start_and_report(&path);
    let baseline = report
        .lines()
        .find_map(|line| line.split("baseline=").nth(1))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("the daemon reported no baseline:\n{report}"));
    assert!(
        baseline > 0,
        "the daemon came back on no baseline at all:\n{report}"
    );

    // And it still knows what it promised. The retained record that
    // answers this retry was materialized into the projection before
    // the image was taken, so it is inside the image; the journal no
    // longer holds the records that produced it.
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x44; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the daemon answered the retry\n{}", daemon.said()));
    let again = response_of(&answer);
    assert_eq!(
        again.command_id, first.command_id,
        "the same invocation became a different command after a reclaimed prefix"
    );
    assert_eq!(
        again.outcome, first.outcome,
        "the node answered differently once its journal prefix was reclaimed"
    );
}

/// `coordd inspect` says what this node's credential is, and starts
/// nothing (task-58).
///
/// The operator-facing half of the credential lifecycle. A peer that
/// refuses a binding tells this node only that it was refused, which is
/// the right amount to tell it; replacing a node is done here, so the
/// distinctions have to be readable here -- "requires-commit
/// committed=1 presented=2" instead of a handshake that fails for
/// reasons the node cannot see.
#[test]
fn inspect_reports_the_credential_against_committed_membership_and_starts_nothing() {
    let dir = workspace("inspect");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let _ = sts_keys(&dir);
    let path = config_only(&dir);

    // The committed generation presenting the committed key: a renewal,
    // which is what a node running normally always is.
    let report = run(&path, &["inspect"]);
    assert_eq!(report.code, Some(0), "{}", report.err);
    assert!(
        report.out.contains("state=renewal"),
        "expected a renewal:\n{}",
        report.out
    );
    assert!(
        report.out.contains("renewal=wait") || report.out.contains("renewal=due"),
        "expected a renewal decision:\n{}",
        report.out
    );
    // Inspecting is not starting: nothing was created, and a node whose
    // credential is wrong must be inspectable without first being
    // repaired.
    assert!(!dir.join("state").exists(), "inspect created a store");
    assert!(!dir.join("journal").exists(), "inspect created a journal");

    // A leaf at the next generation. The certificate is perfectly
    // valid; what is missing is the committed configuration that makes
    // this generation the voter.
    let (next, key) = ca.issue_at(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
        2,
    );
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", next.der())).expect("cert");
    std::fs::write(
        dir.join("node.key"),
        pem("PRIVATE KEY", &key.serialize_der()),
    )
    .expect("key");
    let report = run(&path, &["inspect"]);
    assert_eq!(report.code, Some(0), "{}", report.err);
    assert!(
        report
            .out
            .contains("state=requires-commit committed=1 presented=2"),
        "expected a replacement awaiting its commit:\n{}",
        report.out
    );
    // And the daemon refuses to serve under it rather than starting as
    // a voter nothing committed.
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}", refused.out);

    // The other direction: the configuration moved on and this disk did
    // not. A clone restored from before a replacement is exactly this,
    // and it is the case where serving anyway means a replaced node
    // voting twice.
    genesis_of_incarnation(&dir, &ca.node_spki, 2);
    let stale = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of_incarnation(&dir, &stale.node_spki, 2);
    let report = run(&path, &["inspect"]);
    assert_eq!(report.code, Some(0), "{}", report.err);
    assert!(
        report.out.contains("state=stale committed=2 presented=1"),
        "expected a stale credential:\n{}",
        report.out
    );
    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}", refused.out);
}

/// A genesis committing voter one at `incarnation`.
fn genesis_of_incarnation(dir: &Path, voter_one_key: &[u8], incarnation: u64) {
    let manifest = serde_json::json!({
        "cluster": hex(&CLUSTER),
        "domain": hex(&DOMAIN),
        "epoch": 1,
        "voters": [{
            "node": hex(&[1u8; 16]),
            "incarnation": incarnation,
            "public_key": b64url(voter_one_key),
        }],
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

/// A replacement whose first start fails after it has begun finishes on
/// the next start instead of quarantining the node.
///
/// The start here stops at the checkpoint directory, which is opened
/// after both of the adoption's durable writes; a stop *between* those
/// two writes is `store::tests::an_adoption_stopped_between_its_two_writes_finishes_on_the_next_start`,
/// which is where their order is held while no replacement can reach
/// `coordd` end to end.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "pending a committed reconfiguration path: a key replacement is a committed lifecycle transition (design Section 20.4), and the genesis pin admits no edited manifest, so it cannot be driven through coordd yet"]
async fn a_replacement_whose_first_start_fails_finishes_on_the_next_start() {
    let dir = workspace("replace-interrupted");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    checkpoint_after(&path, 1);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x59; 16]).await;
        let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
        response_of(&answer)
    };

    let (next, key) = ca.issue_at(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
        2,
    );
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", next.der())).expect("cert");
    write_key(&dir.join("node.key"), &key.serialize_der());
    genesis_of_incarnation(&dir, &spki_of(next.der()), 2);

    // The replacement's first start stops partway: something that is not
    // a directory where its checkpoints are, which it finds after the
    // adoption has begun.
    let checkpoints = dir.join("checkpoints");
    let aside = dir.join("checkpoints.aside");
    std::fs::rename(&checkpoints, &aside).expect("move the checkpoints aside");
    std::fs::write(&checkpoints, b"not a directory").expect("obstruct");
    let stopped = run(&path, &[]);
    assert_eq!(stopped.code, Some(2), "{}{}", stopped.out, stopped.err);
    assert!(
        stopped.err.contains("checkpoint directory"),
        "the start did not stop where this test stops it: {}",
        stopped.err
    );

    // The next start finishes the adoption and serves the same answer.
    std::fs::remove_file(&checkpoints).expect("clear");
    std::fs::rename(&aside, &checkpoints).expect("restore the checkpoints");
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x59; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the finished replacement did not serve\n{}", daemon.said()));
    let again = response_of(&answer);
    assert_eq!(again.command_id, first.command_id);
    assert_eq!(
        again.outcome, first.outcome,
        "an interrupted replacement came back on other state"
    );
}

/// An authorized key replacement keeps this node's durable state, and a
/// disk that was left behind by one does not come back (task-58; design
/// Sections 10.4, 20.4).
///
/// The two halves are the same stamp read in the two directions. A node
/// whose voting key is replaced is the same replica: its journal, the
/// checkpoints it published and its epoch metadata are what the new
/// generation has to serve from, and refusing them would make every
/// authorized replacement a restore from nothing. A node whose disk was
/// cloned or restored from *before* a replacement is not the current
/// replica at all, and serving from it is a replaced voter voting.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "pending a committed reconfiguration path: a key replacement is a committed lifecycle transition (design Section 20.4), and the genesis pin admits no edited manifest, so it cannot be driven through coordd yet"]
async fn an_authorized_replacement_keeps_the_state_and_a_left_behind_disk_does_not() {
    let dir = workspace("replace");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    checkpoint_after(&path, 1);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x58; 16]).await;
        let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
        response_of(&answer)
    };

    // Keep the credential this node held, so the other direction can be
    // tried with it afterwards.
    let retired = std::fs::read(dir.join("node.pem")).expect("cert");
    let retired_key = std::fs::read(dir.join("node.key")).expect("key");

    // The replacement: a new leaf at the next generation, and a
    // configuration that commits it. Both are needed -- a certificate
    // nothing committed is refused, and a commitment to a key this node
    // does not hold is refused too.
    let (next, key) = ca.issue_at(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
        2,
    );
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", next.der())).expect("cert");
    write_key(&dir.join("node.key"), &key.serialize_der());
    genesis_of_incarnation(&dir, &spki_of(next.der()), 2);

    // It comes back on its own state, and says so: the adoption is a
    // durable step and an operator replacing a node has to be able to
    // see that it happened.
    let report = start_and_report(&path);
    assert!(
        report.contains("adopted this node's durable state from incarnation 1 under 2"),
        "the replacement did not adopt this node's state:\n{report}"
    );
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x58; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the daemon answered the retry\n{}", daemon.said()));
    let again = response_of(&answer);
    assert_eq!(
        again.command_id, first.command_id,
        "the same invocation became a different command after an authorized replacement"
    );
    assert_eq!(
        again.outcome, first.outcome,
        "the node answered differently after an authorized replacement"
    );
    drop(daemon);

    // And the other direction. The retired credential is perfectly
    // valid and names this very replica; what it does not name is the
    // committed generation, and the root has moved past it. The node
    // refuses to start rather than serving as the voter it no longer
    // is.
    std::fs::write(dir.join("node.pem"), &retired).expect("cert");
    write_key(&dir.join("node.key"), &retired_key);
    let refused = run(&path, &[]);
    assert_eq!(
        refused.code,
        Some(2),
        "a replaced node started on its old credential:\n{}{}",
        refused.out,
        refused.err
    );
    // Told as what it is, so an operator does not read a fenced disk as
    // a broken one.
    let report = run(&path, &["inspect"]);
    assert!(
        report.out.contains("state=stale committed=2 presented=1"),
        "inspect did not name the left-behind credential:\n{}",
        report.out
    );
}

/// A replacement handed to a node as an edited genesis manifest is a
/// genesis quarantine, and it is refused before anything is adopted.
///
/// Adoption is durable and one-way: it carries the journal's stream and
/// advances the projection's manifest, after which the previous
/// credential is fenced. A start that adopted first and checked the pin
/// afterwards would be refused *and* have moved the store, so restoring
/// the manifest the node was initialized under would leave it fenced
/// under its own credential. The pin is checked on the generation as it
/// is stamped, so restoring the manifest and the credential serves the
/// same answer as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_replacement_in_an_edited_genesis_is_quarantined_before_anything_is_adopted() {
    let dir = workspace("replace-pinned");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let first = {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x5a; 16]).await;
        let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
        response_of(&answer)
    };

    let manifest = std::fs::read(dir.join("genesis.json")).expect("manifest");
    let retired = std::fs::read(dir.join("node.pem")).expect("cert");
    let retired_key = std::fs::read(dir.join("node.key")).expect("key");
    let (next, key) = ca.issue_at(
        SERVER_NAME,
        CLUSTER,
        1,
        coord_types::wire_v1::PeerRole::Voter,
        2,
    );
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", next.der())).expect("cert");
    write_key(&dir.join("node.key"), &key.serialize_der());
    genesis_of_incarnation(&dir, &spki_of(next.der()), 2);

    let refused = run(&path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("genesis quarantine"),
        "the refusal did not say why: {}",
        refused.err
    );
    assert!(
        !refused.out.contains("adopted"),
        "a start adopted under a genesis it then refused:\n{}",
        refused.out
    );

    std::fs::write(dir.join("genesis.json"), manifest).expect("restore the manifest");
    std::fs::write(dir.join("node.pem"), &retired).expect("cert");
    // Already PEM, and the file keeps the permissions `write_key` gave it.
    std::fs::write(dir.join("node.key"), &retired_key).expect("key");
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x5a; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| panic!("the restored node did not serve\n{}", daemon.said()));
    let again = response_of(&answer);
    assert_eq!(again.command_id, first.command_id);
    assert_eq!(
        again.outcome, first.outcome,
        "the refused replacement moved this node's state"
    );
}

/// Write a private key with the permissions the daemon insists on.
fn write_key(path: &Path, der: &[u8]) {
    std::fs::write(path, pem("PRIVATE KEY", der)).expect("key");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

/// A backup restores a new cluster and refuses to restore the old one
/// (task-59; design Sections 5.4, 7.4, 17.16).
///
/// The whole runbook end to end on a real node: back up a running
/// domain's state, verify the backup off the disk, refuse the restore
/// without a fencing attestation and under the source identity, and
/// then restore a *different* cluster from the same bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_backup_restores_a_new_cluster_and_refuses_to_restore_the_old_one() {
    let dir = workspace("backup");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    // Something worth restoring.
    {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x59; 16]).await;
        ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
    }

    let out = dir.join("backup");
    let taken = run(&path, &["backup", "--out", out.to_str().unwrap()]);
    assert_eq!(taken.code, Some(0), "{}", taken.err);
    assert!(
        taken.out.contains("recovery_point position="),
        "the backup did not state its recovery point:\n{}",
        taken.out
    );

    // A backup is never written over one. For the duration of an
    // overwrite there would be neither the old backup nor a complete
    // new one, and a backup is exactly the thing that must not be
    // unavailable at the moment it is wanted.
    let twice = run(&path, &["backup", "--out", out.to_str().unwrap()]);
    assert_eq!(
        twice.code,
        Some(2),
        "a backup was written over an existing one:\n{}",
        twice.out
    );

    // A backup is verified from anywhere, including from a machine
    // whose own store is the one that was lost.
    let verified = run(&path, &["verify", "--dir", out.to_str().unwrap()]);
    assert_eq!(verified.code, Some(0), "{}", verified.err);
    assert!(verified.out.contains("backup verified chunks="));

    // And from a machine that is a voter of no cluster: the recovery
    // host's certificate need not be one the committed configuration
    // names, because nothing about a backup's integrity depends on who
    // is asking. Placement refuses this configuration; verification
    // does not go through placement.
    let elsewhere = workspace("backup-elsewhere");
    let elsewhere_config = config(&elsewhere);
    credentials(&elsewhere, 9, coord_types::wire_v1::PeerRole::Voter);
    let stranger = run(&elsewhere_config, &["--check"]);
    assert_eq!(
        stranger.code,
        Some(2),
        "the recovery host is placeable, so this proves nothing:\n{}",
        stranger.out
    );
    let verified = run(
        &elsewhere_config,
        &["verify", "--dir", out.to_str().unwrap()],
    );
    assert_eq!(
        verified.code,
        Some(0),
        "a backup could not be verified from a machine that is not a voter:\n{}",
        verified.err
    );
    assert!(verified.out.contains("backup verified chunks="));

    // A backup index repointed at different bytes fails verification
    // rather than at the restore.
    let tampered = dir.join("tampered");
    copy_tree(&out, &tampered);
    let chunk = tampered.join("chunk-000000");
    let mut bytes = std::fs::read(&chunk).expect("chunk");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&chunk, &bytes).expect("chunk");
    let refused = run(&path, &["verify", "--dir", tampered.to_str().unwrap()]);
    assert_eq!(refused.code, Some(2), "{}", refused.out);

    // The manifest an operator reads, and the attestation they write
    // from it.
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("backup.json")).expect("manifest"))
            .expect("json");
    let root = manifest["root"].clone();

    // The successor: a different cluster, with its own genesis and its
    // own credentials.
    let new_dir = workspace("backup-successor");
    let new_cluster = [0xc5u8; 16];
    let new_ca = credentials_of_cluster(
        &new_dir,
        new_cluster,
        1,
        coord_types::wire_v1::PeerRole::Voter,
    );
    genesis_of_cluster(&new_dir, new_cluster, &new_ca.node_spki);
    let _ = sts_keys(&new_dir);
    let new_path = config_only(&new_dir);

    // Without an attestation there is nothing to restore under: the
    // isolation is an action outside this system and the restore will
    // not assume it happened.
    let unfenced = dir.join("unfenced.json");
    std::fs::write(&unfenced, b"{}").expect("write");
    let refused = run(
        &new_path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            unfenced.to_str().unwrap(),
        ],
    );
    assert_eq!(refused.code, Some(2), "{}", refused.out);

    // An attestation for the old cluster's own identity is a restore in
    // place, which is the one thing that makes a rewound history
    // indistinguishable from the live one.
    let in_place = fencing_file(&dir, "in-place.json", CLUSTER, CLUSTER, &root);
    let refused = run(
        &path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            in_place.to_str().unwrap(),
        ],
    );
    assert_eq!(
        refused.code,
        Some(2),
        "a restore in place was carried out:\n{}",
        refused.out
    );

    // The real one. Planning writes nothing and says what will be lost.
    let attestation = fencing_file(&dir, "fencing.json", CLUSTER, new_cluster, &root);
    let planned = run(
        &new_path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            attestation.to_str().unwrap(),
            "--plan",
        ],
    );
    assert_eq!(planned.code, Some(0), "{}", planned.err);
    assert!(
        planned.out.contains("nothing was written"),
        "a planned restore wrote something:\n{}",
        planned.out
    );
    assert!(
        planned.out.contains(
            "disposition kv=restored-at-boundary retries=restored-at-boundary \
             configurations=not-carried sessions=invalidated leases=revoked \
             watches=resynchronized"
        ),
        "the plan did not state every disposition:\n{}",
        planned.out
    );
    assert!(!new_dir.join("state").exists(), "the plan created a store");

    let restored = run(
        &new_path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            attestation.to_str().unwrap(),
        ],
    );
    assert_eq!(restored.code, Some(0), "{}", restored.err);
    assert!(
        restored.out.contains("restored rows="),
        "the restore said nothing about what it wrote:\n{}",
        restored.out
    );

    // And the new cluster comes up on the restored state, under its
    // own identity: the generation attaches to its journal, the
    // frontiers agree and it reports itself live.
    let report = start_and_report(&new_path);
    assert!(
        report.contains("phase=live"),
        "the restored cluster did not come up:\n{report}"
    );
    assert!(
        report.contains("journaled_through="),
        "the restored projection never attached:\n{report}"
    );

    // Restoring a second time onto the same node is refused: a restore
    // writes a whole cluster's state and has nothing to merge with.
    let again = run(
        &new_path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            attestation.to_str().unwrap(),
        ],
    );
    assert_eq!(again.code, Some(2), "{}", again.out);
}

/// A restore that stopped after its policy and before its pin is refused
/// by a start and finished by `init`, which keeps the restored rows and
/// writes none of the policy twice (task-59).
///
/// A restore ends as `init` does: attach, the successor's genesis
/// policy, and the successor's genesis pinned last. Pinned before the
/// policy, a stop between the two left a restored node that served,
/// trusting nothing and granting nothing, and that nothing would finish.
/// The state a stop just before the pin leaves is a finished restore
/// without its pin, made here by removing it.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_that_stopped_before_its_pin_is_finished_by_init() {
    use coord_store_api::engine::{LocalEngine, WriteTxn};
    use coord_store_api::registry::{Collection, meta_fields};

    let dir = workspace("restore-unpinned");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x59; 16]).await;
        ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered the request\n{}", daemon.said()));
    }
    let out = dir.join("backup");
    let taken = run(&path, &["backup", "--out", out.to_str().unwrap()]);
    assert_eq!(taken.code, Some(0), "{}", taken.err);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("backup.json")).expect("manifest"))
            .expect("json");

    let new_dir = workspace("restore-unpinned-successor");
    let new_cluster = [0xc6u8; 16];
    let new_ca = credentials_of_cluster(
        &new_dir,
        new_cluster,
        1,
        coord_types::wire_v1::PeerRole::Voter,
    );
    genesis_of_cluster(&new_dir, new_cluster, &new_ca.node_spki);
    let _ = sts_keys(&new_dir);
    let new_path = config_only(&new_dir);
    let attestation = fencing_file(
        &dir,
        "fencing.json",
        CLUSTER,
        new_cluster,
        &manifest["root"],
    );
    let restored = run(
        &new_path,
        &[
            "restore",
            "--dir",
            out.to_str().unwrap(),
            "--fencing",
            attestation.to_str().unwrap(),
        ],
    );
    assert_eq!(restored.code, Some(0), "{}", restored.err);
    // The policy is written before the pin, so the restore that finished
    // wrote all of it.
    assert!(
        restored.out.contains(" present=0") && !restored.out.contains("genesis policy rows=0"),
        "the restore did not write the successor's policy before its pin:\n{}",
        restored.out
    );

    // Stopped just before the pin.
    {
        let mut generation = coord_storage_redb::lifecycle::Generation::open_existing(
            &new_dir.join("state"),
            coord_storage_redb::lifecycle::StoreIdentity {
                cluster_id: coord_types::ids::ClusterId(new_cluster),
                domain_id: coord_types::ids::DomainId(DOMAIN),
                replica_id: coord_types::ids::ReplicaId([1; 16]),
                incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
            },
            coord_storage_redb::lifecycle::OpenOptions::default(),
        )
        .expect("the restored store opens");
        let mut txn = generation.engine().begin_write().expect("write");
        txn.delete(Collection::MetaV1.id(), meta_fields::GENESIS_DIGEST)
            .expect("unpin");
        txn.commit_durable().expect("commit");
    }

    let refused = run(&new_path, &[]);
    assert_eq!(refused.code, Some(2), "{}{}", refused.out, refused.err);
    assert!(
        refused.err.contains("never pinned") && refused.err.contains("coordd init"),
        "the refusal did not say what to do: {}",
        refused.err
    );

    let finished = run(&new_path, &["init"]);
    assert_eq!(finished.code, Some(0), "{}{}", finished.out, finished.err);
    assert!(
        finished.out.contains("genesis policy rows=0 present=")
            && !finished.out.contains("present=0"),
        "finishing rewrote a policy the restore had written:\n{}",
        finished.out
    );
    assert!(
        !new_dir.join("state").join("gen-000002").exists(),
        "finishing the restore made a second generation"
    );

    // The finished node comes up on the restored state.
    let report = start_and_report(&new_path);
    assert!(
        report.contains("phase=live") && report.contains("owed=0"),
        "the finished restore did not come up:\n{report}"
    );
    assert!(
        !report.contains("journaled_through=None"),
        "the finished restore came up on no restored state:\n{report}"
    );
}

/// A genesis for another cluster, as a successor deployment has it.
fn genesis_of_cluster(dir: &Path, cluster: [u8; 16], voter_one_key: &[u8]) {
    let manifest = serde_json::json!({
        "cluster": hex(&cluster),
        "domain": hex(&DOMAIN),
        "epoch": 1,
        "voters": [{
            "node": hex(&[1u8; 16]),
            "incarnation": 1,
            "public_key": b64url(voter_one_key),
        }],
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

/// An operator's fencing attestation, as the runbook has them write it.
fn fencing_file(
    dir: &Path,
    name: &str,
    abandoned: [u8; 16],
    successor: [u8; 16],
    backup_root: &serde_json::Value,
) -> PathBuf {
    let path = dir.join(name);
    let attestation = serde_json::json!({
        "abandoned": abandoned,
        "successor": successor,
        "backup": backup_root,
        "action": "revoked the old cluster's node certificates, ticket DR-91",
        "at": 1_700_000_600u64,
    });
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&attestation).expect("json"),
    )
    .expect("write");
    path
}

/// Copy a backup directory, for the tampering case.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("dir");
    for entry in std::fs::read_dir(from).expect("read") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
    }
}

/// A build refuses a store whose cluster has activated something it
/// cannot do, and says so before it could vote (task-60; design
/// Sections 13, 17.7).
///
/// The rollback guard from the node's side. Compatible binaries coexist
/// freely while nothing is active -- that is what makes a rolling
/// upgrade possible -- and activation is the one-way point after which
/// they do not. A node that started anyway would be operating on state
/// it can only half interpret, which is the failure mode no later care
/// recovers from.
#[test]
fn a_node_refuses_a_store_that_activated_something_this_build_cannot_do() {
    let dir = workspace("features");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let _ = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    // The build says what it is, on every start, so a mixed fleet can
    // be reconciled by reading one line per node.
    let report = start_and_report(&path);
    assert!(
        report.contains("build schema=1") && report.contains("features=checkpoint-floor"),
        "the daemon did not report what this build is:\n{report}"
    );

    // Nothing activated: it serves, which is the coexistence half.
    assert_eq!(run(&path, &["--check"]).code, Some(0));

    // Now the cluster activates something this build has never heard
    // of. An activation is written by the cluster, so a build that met
    // an unknown identifier and quietly dropped it would conclude it
    // may serve precisely when it may not.
    write_activation(&dir, &[0x7fff]);
    let refused = run(&path, &[]);
    assert_eq!(
        refused.code,
        Some(2),
        "a node served a store it cannot fully read:\n{}{}",
        refused.out,
        refused.err
    );

    // And one it does support is admitted again: the guard is about
    // capability, not about the record existing.
    write_activation(&dir, &[1]);
    let served = start_and_report(&path);
    assert!(
        served.contains("phase=live"),
        "a node refused a feature it supports:\n{served}"
    );
}

/// Write the cluster's activation record straight into the selected
/// generation, as the replicated path will once task-m02 drives it.
fn write_activation(dir: &Path, features: &[u16]) {
    use coord_store_api::engine::{LocalEngine, WriteTxn};
    let root = dir.join("state");
    let identity = coord_storage_redb::StoreIdentity {
        cluster_id: coord_types::ids::ClusterId(CLUSTER),
        domain_id: coord_types::ids::DomainId(DOMAIN),
        replica_id: coord_types::ids::ReplicaId([1; 16]),
        incarnation: coord_types::ids::ReplicaIncarnation::new(1).unwrap(),
    };
    let options = coord_storage_redb::OpenOptions {
        cache_bytes: 4 << 20,
    };
    let mut generation =
        coord_storage_redb::Generation::open_existing(&root, identity, options).expect("open");
    let record = coord_checkpoint::feature::ActiveFeaturesV1 {
        cluster: coord_types::ids::ClusterId(CLUSTER),
        domain: coord_types::ids::DomainId(DOMAIN),
        configuration: coord_types::ids::ConfigurationEpoch::new(1).unwrap(),
        features: features.to_vec(),
        reporters: vec![coord_types::ids::ReplicaId([1; 16])],
    };
    let mut tx = generation.engine().begin_write().expect("write");
    tx.put(
        coord_store_api::registry::Collection::CheckpointV1.id(),
        coord_checkpoint::feature::ACTIVE_KEY,
        &record.encode().expect("encode"),
    )
    .expect("put");
    tx.commit_durable().expect("commit");
}

/// A running node reports a bounded, secret-free metrics snapshot, and
/// what it does not have says why rather than reporting zero (task-61;
/// design Sections 13, 22.3).
///
/// The snapshot is the operator-facing artifact of the whole
/// observability task, so the test is about the two properties an
/// operator relies on: it describes this node's actual storage, and it
/// contains nothing that could not safely be shipped off the host.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_reports_bounded_secret_free_metrics() {
    let dir = workspace("metrics");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);
    assert_eq!(run(&path, &["init"]).code, Some(0));

    // Serve a write, so the node has a journal position to report, and
    // then restart: the snapshot on the startup report is of the state
    // this process actually recovered to.
    {
        let daemon = start(&path);
        let caller = Caller::bind(&daemon, &ca, &ring, [0x61; 16]).await;
        ask(&caller.connection, &caller.put(1, b"k", b"v"))
            .await
            .unwrap_or_else(|| panic!("the daemon answered\n{}", daemon.said()));
    }
    let said = start_and_report(&path);

    let rendered = said
        .lines()
        .find_map(|line| line.strip_prefix("metrics "))
        .unwrap_or_else(|| panic!("the daemon never reported metrics:\n{said}"));
    let snapshot: serde_json::Value = serde_json::from_str(rendered).expect("valid json");

    // It describes this node's actual storage: a node that served a
    // write has a journal position, and the three frontiers are
    // reported separately because they are three different facts.
    let frontiers = snapshot
        .pointer("/frontiers/Observed")
        .expect("the frontiers are observed on a node with a store");
    assert!(
        frontiers["journal"].as_u64().expect("a journal head") > 0,
        "a node that served a write reported an empty journal: {frontiers}"
    );
    assert!(frontiers.get("materialized").is_some());
    assert!(frontiers.get("checkpoint").is_some());

    // What it does not have says why, never zero. This build times no
    // view and no durability quantity, and configures no engine bound,
    // and each is a stated absence.
    for (field, reason) in [
        ("/view_age/Unavailable", "NotInstrumented"),
        ("/engine_pressure/Unavailable", "NoBound"),
        ("/durability/Unavailable", "NotInstrumented"),
    ] {
        assert_eq!(
            snapshot.pointer(field).and_then(|v| v.as_str()),
            Some(reason),
            "{field} was not a stated absence:\n{rendered}"
        );
    }

    // Every stage is present, and this node's roles decide which are
    // observed: a missing series is always a stated absence.
    let stages = snapshot["stages"].as_array().expect("stages");
    assert_eq!(stages.len(), 12, "a stage was dropped rather than stated");
    // The stages this daemon records are observed, and the ones nothing
    // in it records say so rather than reporting counts nobody took.
    let reading = |name: &str| {
        stages
            .iter()
            .find(|s| s["stage"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("no {name} reading:\n{rendered}"))["metrics"]
            .clone()
    };
    for recorded in ["Admission", "Journal", "Materialization"] {
        assert!(
            reading(recorded).get("Observed").is_some(),
            "{recorded} is recorded by this daemon but was not observed:\n{rendered}"
        );
    }
    for unrecorded in ["FanOut", "ClientTransit", "EvidenceLearning"] {
        assert_eq!(
            reading(unrecorded)
                .pointer("/Unavailable")
                .and_then(|v| v.as_str()),
            Some("NotInstrumented"),
            "{unrecorded} has no instrumentation point but was reported:\n{rendered}"
        );
    }

    // And nothing in it could not be shipped off the host. The scan is
    // the one design Section 22.3 asks for; it finds nothing because
    // nothing of that kind is ever recorded.
    for pattern in [
        "BEGIN", "PRIVATE", "Bearer", "eyJ", "secret", "token", "password",
    ] {
        assert!(
            !rendered.contains(pattern),
            "the snapshot contains {pattern:?}:\n{rendered}"
        );
    }
    let longest = rendered
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(
        longest <= 20,
        "the snapshot carries a {longest}-character run, which is identity-shaped:\n{rendered}"
    );
}

/// A watch opened against the running daemon is served the events of
/// the revisions that follow it, on the stream it was opened on.
///
/// This is the one request shape whose stream outlives its frame. The
/// caller opens it once; the events are written onto that same stream
/// for the life of the subscription, which means the daemon has to hold
/// the responder and keep moving what the hub produces onto it rather
/// than answering once and letting the stream go.
///
/// Two writes, because one proves less than it looks: a single event
/// could be delivered by a pump that happens to run when the request
/// that caused it is answered. The second arrives with nothing else
/// going on, so it is the subscription that delivered it.
///
/// What this does not test is resumption from history: the watch here
/// starts at the frontier, and replaying a compacted or retained past
/// is the certification suite's, where a real API server's reflector
/// asks for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_watch_is_served_the_revisions_that_follow_it() {
    let dir = workspace("watch");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x77; 16]).await;

    // The subscription's own stream. It is not finished: the caller has
    // said everything it has to say, and everything else on this stream
    // comes the other way.
    let (mut send, mut recv) = caller
        .connection
        .open_bi()
        .await
        .expect("a stream for the watch");
    let open = coord_types::wire_v1::MessageV1::WatchOpen(coord_types::wire_v1::WatchOpenV1 {
        watch_id: 7,
        namespace: coord_types::ids::NamespaceId(REQUEST_NAMESPACE),
        key: coord_types::wire_v1::BoundedBytes::new(b"w".to_vec()).expect("bounded"),
        range_end: None,
        // From the start of history, which is what a resuming client
        // asks for and what makes this deterministic: the open and the
        // first write are on different streams, so which of them the
        // daemon sees first is the network's to decide. A watch from a
        // revision is delivered either way -- replayed when it attached
        // above the write, live when below -- and one that attached at
        // whatever the frontier happened to be would be a test of that
        // race instead.
        start_revision: Some(coord_types::ids::KvRevision::new(1).expect("nonzero")),
        prev_kv: false,
        progress_notify: false,
    })
    .encode()
    .expect("bounded");
    send.write_all(&open).await.expect("the open is written");
    // The request half ends here, as it does for every request: a
    // caller says one thing on a stream it opens. The receiving half is
    // what stays, and it is the subscription.
    send.finish().expect("the open is complete");

    let mut reader = coord_types::wire_v1::FrameReader::new();
    let mut buffered: Vec<coord_types::wire_v1::Frame> = Vec::new();
    let mut seen: Vec<(u64, Vec<u8>)> = Vec::new();
    for (sequence, value) in [(1u64, b"one".to_vec()), (2, b"two".to_vec())] {
        let answer = ask(&caller.connection, &caller.put(sequence, b"w", &value))
            .await
            .expect("the daemon never answered the write");
        let coord_types::wire_v1::MessageV1::Response(response) =
            coord_types::wire_v1::decode(&answer).expect("a decodable answer")
        else {
            panic!("the daemon answered a write with something else");
        };
        assert!(
            matches!(response.outcome, coord_types::wire_v1::OutcomeV1::Ok { .. }),
            "the write did not execute: {response:?}"
        );

        // One event frame per write, read off the subscription's stream
        // within a bound. A watch that is registered and never pumped
        // fails here by timing out, which is what this build did before
        // the daemon held the stream.
        let frame = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(frame) = buffered.pop() {
                    return Some(frame);
                }
                if let Some(frame) = reader.next_frame().expect("a frame") {
                    return Some(frame);
                }
                let mut buf = [0u8; 4096];
                match recv.read(&mut buf).await {
                    Ok(Some(n)) => reader.push(&buf[..n]).expect("within the reader bound"),
                    // The daemon ended the stream, or the connection.
                    // Either is a failure of the subscription, and what
                    // the daemon said about it is the diagnosis.
                    Ok(None) => return None,
                    Err(_) => return None,
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the watch delivered nothing within the bound:\n{}",
                daemon.said()
            )
        })
        .unwrap_or_else(|| panic!("the daemon ended the watch stream:\n{}", daemon.said()));
        match coord_types::wire_v1::decode(&frame).expect("a decodable watch frame") {
            coord_types::wire_v1::MessageV1::WatchEvents(events) => {
                assert_eq!(events.watch_id, 7, "another subscription's events");
                assert!(events.complete, "a revision arrived in pieces");
                let delivered: Vec<Vec<u8>> = events
                    .events
                    .as_slice()
                    .iter()
                    .map(|e| e.value.as_slice().to_vec())
                    .collect();
                seen.push((events.revision.get(), delivered.concat()));
            }
            other => panic!("the watch delivered something else: {other:?}"),
        }
    }

    // In order, each revision once, carrying what was written.
    assert_eq!(
        seen.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec![b"one".to_vec(), b"two".to_vec()],
        "the watch delivered {seen:?}"
    );
    assert!(
        seen[0].0 < seen[1].0,
        "the watch delivered revisions out of order: {seen:?}"
    );
}

/// Two callers asking at the same time are both served by a quorum.
///
/// One caller at a time is the shape every earlier test has: ask, wait,
/// ask again. A domain can serialize all of its work and still pass
/// every one of them. An API server is concurrent from its first
/// second, so this asks two sessions to keep one request each in flight
/// against three voters and expects every one of them answered.
#[tokio::test(flavor = "multi_thread")]
async fn two_callers_at_once_are_both_served_by_a_quorum() {
    let dir = workspace("concurrent");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0), "each store is made");
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "voter {} cannot submit to the other two:\n{}",
            n + 1,
            node.said()
        );
    }

    let first = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x41; 16]).await;
    let second = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x42; 16]).await;

    // Each caller keeps one request in flight and writes its own keys,
    // so nothing here contends for a value: what is shared is the
    // domain's ordering, not the data.
    const EACH: usize = 25;
    async fn ask_all(caller: &Caller, tag: u8) -> usize {
        let mut answered = 0;
        for n in 0..EACH {
            let key = format!("{tag:02x}-{n:04}").into_bytes();
            if ask(&caller.connection, &caller.put(n as u64 + 1, &key, b"v"))
                .await
                .is_some()
            {
                answered += 1;
            }
        }
        answered
    }
    let (a, b) = tokio::join!(ask_all(&first, 1), ask_all(&second, 2));
    assert_eq!(
        (a, b),
        (EACH, EACH),
        "two callers at once were answered {a} and {b} of {EACH} each\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
        running[0].said(),
        running[1].said(),
        running[2].said()
    );
}

/// A key written under a time to live stops being readable, and the
/// domain says so because a replicated command deleted it.
///
/// This is the whole of the private-TTL path end to end: a Kine create
/// carries a TTL and the hidden binding it derives, the leader's
/// scheduler arms a deadline under an authority epoch it ordered, and
/// when the deadline passes it proposes a conditional expiry that every
/// replica applies at its own position. Nothing here deletes a key
/// locally because a timer went off.
///
/// The wait is generous on purpose. Expiry is allowed to be late --
/// the deadline waits `(1 + rho) * TTL` local ticks and the scheduler
/// reads committed state on an interval -- and it is not allowed to be
/// early, which the first read is there to check. Nothing but the
/// deadline may make it happen, which is why the wait is idle.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_under_a_time_to_live_stops_being_readable() {
    let dir = workspace("ttl");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x7c; 16]).await;

    let namespace = coord_types::ids::NamespaceId(REQUEST_NAMESPACE);
    let key = b"ttl-key".to_vec();
    let create = |sequence: u64| {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            namespace,
            coord_types::logical_v1::CanonicalOperation::KineCreate(
                coord_types::logical_v1::KineCreateOp {
                    key: key.clone(),
                    value: b"v".to_vec(),
                    ttl_seconds: 1,
                    // The collector derives this from the stable
                    // request; what matters here is that it is the
                    // key's own binding and never reaches the caller.
                    binding: Some(coord_types::ids::LeaseId([0xb1; 16])),
                },
            ),
        );
        logical.canonicalize();
        coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(caller.invocation(sequence), &logical, 0, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded")
    };
    let read = |sequence: u64| {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            namespace,
            coord_types::logical_v1::CanonicalOperation::Range(coord_types::logical_v1::RangeOp {
                range: coord_types::logical_v1::KeyRange::exact(key.clone()),
                revision: None,
                limit: 0,
                keys_only: false,
                count_only: false,
            }),
        );
        logical.canonicalize();
        coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(caller.invocation(sequence), &logical, 0, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded")
    };
    /// The rows a read answered with.
    fn rows(frame: &coord_types::wire_v1::Frame) -> u64 {
        let response = response_of(frame);
        let coord_types::wire_v1::OutcomeV1::Ok { result, .. } = &response.outcome else {
            panic!("the read failed: {response:?}");
        };
        let decoded: coord_state::Response =
            postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
        match decoded.outcome {
            coord_state::Outcome::Range { count, .. } => count,
            other => panic!("a read answered with {other:?}"),
        }
    }

    let answer = ask(&caller.connection, &create(1))
        .await
        .expect("the daemon never answered the write");
    let response = response_of(&answer);
    assert!(
        matches!(response.outcome, coord_types::wire_v1::OutcomeV1::Ok { .. }),
        "the write did not execute: {response:?}"
    );

    // Present immediately: a deadline is not permission, and nothing
    // may delete the key before its time.
    let present = ask(&caller.connection, &read(2))
        .await
        .expect("the daemon never answered the read");
    assert_eq!(rows(&present), 1, "the key was gone before its time");

    // And gone once the deadline passes -- on a domain nobody touched
    // meanwhile. The wait is idle on purpose, followed by one read: the
    // expiry has to happen because its deadline passed, not because
    // traffic arrived, and a read that arrived first and was ordered
    // ahead of the expiry would see the key. Five seconds covers the
    // one-second time to live, the clock-rate margin, the scan interval
    // and the time to order the expiry, with room to spare on a slow
    // host.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let answer = ask(&caller.connection, &read(3))
        .await
        .expect("the daemon never answered the read");
    assert_eq!(
        rows(&answer),
        0,
        "the key was still readable after an idle wait well past its time to live:\n{}",
        daemon.said()
    );
}

/// A caller naming one of the service's own operations is refused,
/// whatever its session may do.
///
/// The two service operations are narrow by design, but narrowness is
/// not what keeps a caller out of them: what does is the admission
/// beside the payload. Every submission a collector makes carries a
/// receipt minted for a session, and these execute only for a command
/// accepted with no receipt at all -- which only a voter's own proposal
/// is. So this asks for the one thing a caller must never be able to
/// ask for: the deletion of somebody's key, spelled as an expiry, with
/// an authority epoch it chose itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_cannot_expire_a_lease_however_it_spells_it() {
    let dir = workspace("service-op");
    let ca = credentials(&dir, 1, coord_types::wire_v1::PeerRole::Voter);
    genesis_of(&dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(&dir);
    let path = config_only(&dir);

    assert_eq!(run(&path, &["init"]).code, Some(0));
    let daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x5e; 16]).await;

    for (n, operation) in [
        coord_types::logical_v1::CanonicalOperation::ExpireLease {
            lease_id: coord_types::ids::LeaseId([0xb1; 16]),
            generation: coord_types::ids::LeaseGeneration::new(1).expect("nonzero"),
            expected_renewal_sequence: 0,
            authority_epoch: coord_types::ids::LeaseAuthorityEpoch::new(1).expect("nonzero"),
        },
        coord_types::logical_v1::CanonicalOperation::EstablishLeaseAuthority {
            epoch: coord_types::ids::LeaseAuthorityEpoch::new(99).expect("nonzero"),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut logical = coord_types::logical_v1::LogicalRequest::new(
            coord_types::ids::NamespaceId(REQUEST_NAMESPACE),
            operation,
        );
        logical.canonicalize();
        let frame = coord_types::wire_v1::MessageV1::Request(
            coord_types::wire_v1::RequestV1::new(caller.invocation(n as u64 + 1), &logical, 0, 0)
                .expect("bounded"),
        )
        .encode()
        .expect("bounded");
        let answer = ask(&caller.connection, &frame)
            .await
            .expect("the daemon never answered");
        let response = response_of(&answer);
        // Refused as a replicated outcome, at the position the command
        // already has: a command chosen to execute is never left
        // unexecuted, and the refusal changes nothing.
        let coord_types::wire_v1::OutcomeV1::Ok { result, revision } = &response.outcome else {
            panic!("the daemon answered with a transport-level error: {response:?}");
        };
        assert!(
            revision.is_none(),
            "a refused service operation consumed a revision: {response:?}"
        );
        let executed: coord_state::Response =
            postcard::from_bytes(result.as_slice()).expect("the replicated result decodes");
        assert_eq!(
            executed.outcome,
            coord_state::Outcome::ErrRejected {
                reason: coord_state::RejectionReason::AdmissionMismatch
            },
            "a caller's service operation was not refused for the right reason"
        );
    }
}

/// A replica that falls behind catches up, and its own callers are
/// answered while it does.
///
/// A voter that learns a command's identity before its content asks a
/// peer for the payload, and until that arrives it executes nothing
/// past it. The ask is bounded at `MAX_PAYLOAD_TRANSFER` commands and
/// travels on the bulk lane, so the rate it is *repeated* at decides
/// whether the catch-up path works or eats itself: a replica that asks
/// again before its last batch is answered puts a fresh batch of eight
/// on that lane for every payload that lands, the lane fills, the
/// answers are dropped, and it falls further behind for having asked.
///
/// The load here is what makes a replica fall behind at all -- sustained,
/// read-heavy, and spread over all three frontends so every voter is
/// serving as well as voting. What this asserts is the visible
/// consequence: nothing wedges, no voter reports a bulk lane it could
/// not queue on, and every caller is answered. Driving the same load
/// with the ask repeated on every partial answer fills one voter's log
/// with `QueueFull { lane: Bulk }` and leaves a third of the reads
/// unanswered.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_that_falls_behind_catches_up_without_starving_its_own_catch_up() {
    let dir = workspace("catch-up-quorum");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0), "each store is made");
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "voter {} cannot submit to the other two:\n{}",
            n + 1,
            node.said()
        );
    }

    // Two callers per frontend, so a voter that stops keeping up is a
    // voter whose own callers stop being answered -- which is how this
    // shows up in a deployment and is invisible when every caller binds
    // to the same node -- and so there is enough concurrency for a
    // replica to learn a command's identity before its content.
    const PER_FRONTEND: usize = 2;
    let mut callers = Vec::new();
    for (n, node) in running.iter().enumerate() {
        for k in 0..PER_FRONTEND {
            let session = [0x60 + (n * PER_FRONTEND + k) as u8; 16];
            callers.push(Caller::bind(node, &cluster.ca, &cluster.ring, session).await);
        }
    }

    // Writes to put every voter well past its table's capacity, so the
    // ones that are behind have payloads to ask for, and then reads,
    // which are what a replica that has stopped keeping up cannot
    // answer: a read is served from this node's own projection.
    const WRITES: u64 = 120;
    const READS: u64 = 60;
    async fn load(caller: &Caller, tag: u8) -> u64 {
        let mut answered = 0;
        for n in 1..=WRITES {
            let key = format!("b{tag:02x}-{n:04}").into_bytes();
            if ask(&caller.connection, &caller.put(n, &key, b"v"))
                .await
                .is_some()
            {
                answered += 1;
            }
        }
        for n in 1..=READS {
            let prefix = format!("b{:02x}-", (n % 6) as u8 + 1).into_bytes();
            if ask(&caller.connection, &caller.scan(WRITES + n, &prefix))
                .await
                .is_some()
            {
                answered += 1;
            }
        }
        answered
    }
    // Bounded, because the failure this guards against is a domain
    // that stops making progress rather than one that answers wrongly.
    // Without a bound a build with the defect does not fail here, it
    // hangs, and a gate that hangs tells an operator less than one that
    // fails.
    let all = tokio::time::timeout(Duration::from_secs(120), async {
        tokio::join!(
            load(&callers[0], 1),
            load(&callers[1], 2),
            load(&callers[2], 3),
            load(&callers[3], 4),
            load(&callers[4], 5),
            load(&callers[5], 6)
        )
    })
    .await;
    let Ok((a, b, c, d, e, f)) = all else {
        panic!(
            "the load never finished, so the domain stopped making progress\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
            running[0].said(),
            running[1].said(),
            running[2].said()
        );
    };
    let answered = vec![a, b, c, d, e, f];
    assert_eq!(
        answered,
        vec![WRITES + READS; callers.len()],
        "each caller was answered {answered:?} of {} \n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
        WRITES + READS,
        running[0].said(),
        running[1].said(),
        running[2].said()
    );

    // No voter may report a sustained bulk lane it could not queue on.
    // That is the catch-up path starving itself: the answers to a
    // replica's asks are what fills that lane, so a node that cannot
    // queue on it is one whose peer asked faster than it could be
    // answered. A lane that fills and drains inside a turn is ordinary
    // and says nothing; the line appears only past
    // `UNDELIVERABLE_SAID_AT` frames and then at each doubling, so its
    // presence at all is the finding.
    let said: Vec<String> = running.iter().map(Running::said).collect();
    for (n, s) in said.iter().enumerate() {
        let complained: Vec<&str> = s
            .lines()
            .filter(|l| l.contains("QueueFull { lane: Bulk }"))
            .collect();
        assert!(
            complained.is_empty(),
            "voter {} could not queue on the lane its catch-up travels on: {:?}\n{}",
            n + 1,
            complained,
            s
        );
    }

    // And the domain still answers a caller that arrives afterwards,
    // on every frontend: a replica that quietly stopped executing
    // serves reads from a projection that has stopped moving, and the
    // first thing that shows it is a binding that is never answered.
    for (n, node) in running.iter().enumerate() {
        let later = Caller::bind(node, &cluster.ca, &cluster.ring, [0x70 + n as u8; 16]).await;
        assert!(
            ask(&later.connection, &later.put(1, b"after", b"v"))
                .await
                .is_some(),
            "voter {} never answered a caller that arrived after the load\n{}",
            n + 1,
            node.said()
        );
    }
}

/// A quorum goes on answering long past its command table's capacity.
///
/// The single-voter version of this found the reclamation defect. This
/// is the same question of a real quorum, where the answer depends on
/// every replica draining: a follower that stops executing stops
/// retiring, its table fills, and from then on it refuses every
/// submission and every proposal it is sent -- silently, because a
/// refusal at that door is not something the caller is told.
#[tokio::test(flavor = "multi_thread")]
async fn a_quorum_keeps_answering_past_its_table_capacity() {
    let dir = workspace("sustained-quorum");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0), "each store is made");
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "voter {} cannot submit to the other two:\n{}",
            n + 1,
            node.said()
        );
    }
    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x43; 16]).await;

    // Comfortably past the table's capacity, which is 64.
    const ASKS: u64 = 400;
    for n in 1..=ASKS {
        let key = format!("k{n:04}").into_bytes();
        let answered = ask(&caller.connection, &caller.put(n, &key, b"v")).await;
        assert!(
            answered.is_some(),
            "request {n} of {ASKS} was never answered\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
            running[0].said(),
            running[1].said(),
            running[2].said()
        );
    }

    // And again with two callers in flight, which is what fills a
    // follower's table with the leader's proposals as well as its own
    // records. A follower in that state reclaims ahead of the leader,
    // and the next proposal names the command it has just retired: the
    // dependency guard has to read that as executed, because it was.
    // Four, not two. What this phase is really for is the race between
    // a voter hearing a command's identity from the leader and hearing
    // its content from the collector, and the more callers there are
    // the more often the first wins.
    const EACH: u64 = 120;
    const CALLERS: usize = 4;
    let mut callers = Vec::new();
    for n in 0..CALLERS {
        let session = [0x44 + n as u8; 16];
        callers.push(Caller::bind(&running[0], &cluster.ca, &cluster.ring, session).await);
    }
    async fn ask_all(caller: &Caller, tag: u8) -> u64 {
        let mut answered = 0;
        for n in 1..=EACH {
            let key = format!("c{tag:02x}-{n:04}").into_bytes();
            if ask(&caller.connection, &caller.put(n, &key, b"v"))
                .await
                .is_some()
            {
                answered += 1;
            }
        }
        answered
    }
    let mut work = Vec::new();
    for (n, caller) in callers.iter().enumerate() {
        work.push(ask_all(caller, n as u8 + 1));
    }
    // Every caller keeps one request in flight at a time, and all of
    // them are in flight at once.
    let (a, b, c, d) = tokio::join!(
        work.remove(0),
        work.remove(0),
        work.remove(0),
        work.remove(0)
    );
    let answered = vec![a, b, c, d];
    assert_eq!(
        answered,
        vec![EACH; CALLERS],
        "{CALLERS} callers past the capacity were answered {answered:?} of {EACH} each\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
        running[0].said(),
        running[1].said(),
        running[2].said()
    );

    // The symptom of the defect was not a slow domain but a dead one:
    // every voter's table stayed full, so the next caller's session --
    // a replicated command like any other -- was refused at a door
    // that tells the caller nothing, and the binding waited out its
    // deadline. So the last thing asked here is a new session.
    let later = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x50; 16]).await;
    assert!(
        ask(&later.connection, &later.put(1, b"after", b"v"))
            .await
            .is_some(),
        "a caller that arrived after the load was never answered\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
        running[0].said(),
        running[1].said(),
        running[2].said()
    );

    // Two callers at once is also what makes a voter hear about a
    // command from a peer before the collector asks it for one, so this
    // run is where the holding path is exercised. The assertions above
    // are the ones that matter; these two say that they were not
    // passed vacuously, and that nothing was lost on the way.
    let said: Vec<String> = running.iter().map(Running::said).collect();
    assert!(
        said.iter()
            .any(|s| s.contains("holding evidence for a submitter it does not know yet")),
        "no voter ever held evidence, so this run did not exercise the path it is here for\n-- voter 1 --\n{}\n-- voter 2 --\n{}\n-- voter 3 --\n{}",
        said[0],
        said[1],
        said[2]
    );
    // What must not happen is the hold's own bound being reached. Under
    // this load a collector's fan-out is sometimes more than a peer's
    // lane can queue, the transport drops it, and that voter is never
    // told where the evidence for that command belongs -- so the hold
    // expiring is ordinary here and is not what this asserts. The bound
    // is sized for one hold's worth of that; reaching it would mean the
    // bound and the traffic had diverged.
    for (n, s) in said.iter().enumerate() {
        assert!(
            !s.contains("dropped held evidence for want of room"),
            "voter {} ran out of room to hold evidence in:\n{}",
            n + 1,
            s
        );
    }
    // That the hold is released on its window, not merely filled, is
    // not asserted here. A release needs a submission the transport
    // dropped under this load, and whether one is dropped on a given
    // run, to a given voter, is not something this test controls: an
    // assertion on it is a race outcome, not an invariant. The release
    // is covered deterministically instead, at three levels:
    // `coord-daemon`'s `parked.rs` (the next expiry is the oldest hold,
    // and moves on when it goes), `serve.rs`'s
    // `held_evidence_registers_its_own_next_expiry` (the serving loop
    // registers that deadline), and `repair.rs`'s
    // `a_duplicate_after_the_hold_expired_repairs_the_callers_evidence`
    // (three voters over real stores let a hold expire and repair).
    // What only this load shows stays above: the table does not fill,
    // every caller is answered, and the hold's bound is never reached.
}

/// Links a connection's age cap ends are dialled again, on both planes,
/// with no restart (task-d03).
///
/// The transport ends every connection at its age cap, twelve hours by
/// default and shortened here to three seconds. Dialled only when the
/// serving loop started, a mesh had no way back from that but restarting
/// its nodes: half a day after start the domain went quiet. Each voter
/// here loses every link it holds, on the peer plane -- both of its lanes,
/// control and bulk -- and on the api plane it submits over, and holds
/// them all again, and a request asked after the caps have run is
/// established by all three.
///
/// Debug builds only: the cap is set through a knob a release build does
/// not compile.
#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread")]
async fn links_the_age_cap_ends_are_dialled_again_on_both_planes() {
    let dir = workspace("age-cap");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0));
    }
    let running: Vec<Running> = cluster
        .configs
        .iter()
        .map(|c| start_with(c, &[("COORDD_TEST_CONNECTION_AGE_MS", "3000")]))
        .collect();
    for (n, node) in running.iter().enumerate() {
        assert!(
            node.waits_until(40, |said| healed(said, "peers connected=", 2)
                && came_back(&reported_bulk(said), 2)
                && healed(said, "voters submittable=", 2)),
            "voter {} did not get its links back after the age cap ended them:\n{}",
            n + 1,
            node.said()
        );
    }
    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x4d; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| {
            panic!(
                "the healed mesh did not establish a request\n-- 1 --\n{}\n-- 2 --\n{}\n-- 3 --\n{}",
                running[0].said(),
                running[1].said(),
                running[2].said()
            )
        });
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));
}

/// A voter that is stopped and started again is dialled again by the
/// ones that stayed up, on both planes (task-d03).
///
/// The restarted voter dials its peers itself when it starts, which
/// brings its peer-plane links back from its own end. Its *collector*
/// links from the others are what nobody restored: an inbound collector
/// connection carries no replica identity, so the restarted voter's own
/// dials do not make it submittable from the survivors. They have to
/// dial it, and before this they never did.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_voter_is_dialled_again_by_the_survivors() {
    let dir = workspace("restart-voter");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0));
    }
    let mut running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for node in &running {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "{}",
            node.said()
        );
    }
    // Voter 3 goes away, and the survivors see it go. Killed, it closes
    // nothing, so its links end at the transport's idle timeout.
    drop(running.pop());
    for node in &running {
        assert!(
            node.waits_until(50, |said| reported(said, "voters submittable=").last()
                == Some(&1)),
            "a survivor did not see voter 3 go:\n{}",
            node.said()
        );
    }
    running.push(start(&cluster.configs[2]));
    for (n, node) in running.iter().take(2).enumerate() {
        assert!(
            node.waits_until(30, |said| healed(said, "voters submittable=", 2)
                && reported(said, "peers connected=").last() == Some(&2)),
            "survivor {} did not dial the restarted voter again:\n{}",
            n + 1,
            node.said()
        );
    }
}

/// A submission made while the voters it needs were unreachable is
/// delivered when they come back, by its re-offer, and the caller gets
/// its answer without asking again (task-d03).
///
/// Voter 1 is left alone: a put it takes cannot be established, because
/// a majority is two. Voter 2 then comes back. Voter 1 has to dial it --
/// its collector link to voter 2 died with voter 2 -- and the re-offer
/// that was backing off has to go out when the link returns. Without the
/// re-dial the caller waits for ever: voter 2's own dials give it a
/// peer-plane link to voter 1, never a submission link from it.
#[tokio::test(flavor = "multi_thread")]
async fn a_submission_made_while_its_voters_were_away_is_delivered_when_they_return() {
    let dir = workspace("reoffer-return");
    let cluster = three_voters(&dir);
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0));
    }
    let mut running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for node in &running {
        assert!(
            node.waits_to_say("voters submittable=2 of 2"),
            "{}",
            node.said()
        );
    }
    // The session is bound while a majority is there: binding is itself
    // a replicated command.
    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x4e; 16]).await;
    drop(running.pop());
    drop(running.pop());
    // Both planes, not only the submission links. A peer-plane link voter
    // 1 dialled itself outlives the killed voter until the idle timeout,
    // and while it is held a restarted voter 2's own dials lose the
    // collision to it: the surviving connection is the one the lower
    // replica identity dialled. Restarted inside that window, voter 2
    // would reach the peer plane only when the stale link ended, which is
    // the transport's idle timeout and not this test's subject.
    assert!(
        running[0].waits_until(50, |said| reported(said, "voters submittable=").last()
            == Some(&0)
            && reported(said, "peers connected=").last() == Some(&0)),
        "voter 1 did not see the others go:\n{}",
        running[0].said()
    );

    let put = caller.put(1, b"k", b"v");
    let connection = caller.connection.clone();
    let asked = tokio::spawn(async move { ask(&connection, &put).await });
    // Long enough for the offer to voter 2 to have failed and backed off.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!asked.is_finished(), "a lone voter answered a put");

    let back = start(&cluster.configs[1]);
    let returned = std::time::Instant::now();
    let answer = asked.await.expect("the ask ran").unwrap_or_else(|| {
        panic!(
            "the submission was never delivered to the returning voter\n-- 1 --\n{}\n-- 2 --\n{}",
            running[0].said(),
            back.said()
        )
    });
    eprintln!(
        "answered {:?} after voter 2 was serving again",
        returned.elapsed()
    );
    eprintln!("-- 1 --\n{}\n-- 2 --\n{}", running[0].said(), back.said());
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));
}

// ---------------------------------------------------------------------
// Leaf renewal inside the serving daemon (task-d02).
// ---------------------------------------------------------------------

/// Wall-clock seconds, as a leaf's validity is stated.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs()
}

impl Ca {
    /// A leaf for `key` that is valid from a second ago for `secs`
    /// seconds, with the names and node identity [`Ca::issue`] gives.
    ///
    /// Short on purpose: renewal is continuous only because leaves are
    /// short-lived, and a test that wants to watch a node renew, or fail
    /// to, has to hold a leaf that ends while it watches. Returns the
    /// certificate and its `notAfter`.
    fn issue_short(&self, key: &rcgen::KeyPair, replica: u8, secs: u64) -> (Vec<u8>, u64) {
        self.issue_short_as(key, replica, secs, coord_types::wire_v1::PeerRole::Voter)
    }

    /// The same, for `role`: a collector's leaf names the node as
    /// `Frontend`.
    fn issue_short_as(
        &self,
        key: &rcgen::KeyPair,
        replica: u8,
        secs: u64,
        role: coord_types::wire_v1::PeerRole,
    ) -> (Vec<u8>, u64) {
        let identity = coord_node_issuer::NodeIdentity {
            cluster: coord_types::ids::ClusterId(CLUSTER),
            node: coord_types::ids::ReplicaId([replica; 16]),
            incarnation: coord_types::ids::ReplicaIncarnation::new(1).expect("positive"),
            role,
        };
        let now = unix_now();
        let not_after = now + secs - 1;
        let mut params =
            rcgen::CertificateParams::new(vec![SERVER_NAME.to_string()]).expect("leaf params");
        params.not_before =
            time::OffsetDateTime::from_unix_timestamp(now as i64 - 1).expect("representable");
        params.not_after =
            time::OffsetDateTime::from_unix_timestamp(not_after as i64).expect("representable");
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.subject_alt_names = vec![
            rcgen::SanType::DnsName(SERVER_NAME.try_into().expect("dns name")),
            rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            rcgen::SanType::URI(
                coord_node_issuer::node_uri(&identity)
                    .try_into()
                    .expect("uri"),
            ),
        ];
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let issuer = rcgen::Issuer::from_params(&ca_params, &self.key);
        let certificate = params.signed_by(key, &issuer).expect("leaf cert");
        (certificate.der().to_vec(), not_after)
    }
}

/// The platform identity the issuer's policy matches, and the audience
/// its assertions carry.
const K8S_ISS: &str = "https://kubernetes.default.svc";
const ENROLL_AUD: &str = "node-enrollment";

/// A node issuer serving this domain's CA over plain HTTP on loopback,
/// in this test's process: the same `coord_node_issuer::http::router` a
/// deployment runs, with a workload identity provider whose assertion
/// the node reads from a file.
struct NodeIssuerFixture {
    state: std::sync::Arc<coord_node_issuer::IssuerState>,
    /// The workload identity provider's key, which signs assertions.
    idp: rcgen::KeyPair,
}

impl NodeIssuerFixture {
    /// An issuer signing with `ca`, whose policy lets the workload
    /// `voters/voter-<n>` enroll node `n` as a voter, and as that node's
    /// collector (`Frontend`), for `nodes`.
    ///
    /// The voter rule comes first, so a request that did not say which
    /// role it asks for would be answered by it, and a collector's
    /// renewal refused for coming back as a voter.
    fn new(ca: &Ca, nodes: &[u8]) -> Self {
        let now = unix_now();
        let idp = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("idp key");
        let point = idp.public_key_raw();
        let jwks = serde_json::to_vec(&serde_json::json!({"keys": [{
            "kty": "EC", "crv": "P-256", "kid": "kk", "alg": "ES256", "use": "sig",
            "x": b64url(&point[1..33]), "y": b64url(&point[33..65]),
        }]}))
        .expect("jwks");
        let config = coord_authn::IssuerConfig {
            name: "k8s".into(),
            issuer: K8S_ISS.into(),
            jwks_url: "https://k8s/keys".into(),
            algorithms: vec![jsonwebtoken::Algorithm::ES256],
            audiences: vec![ENROLL_AUD.into()],
            max_age_secs: Some(3600),
            allow_insecure_loopback: false,
        };
        let mut registry =
            coord_authn::Registry::new(vec![config], coord_authn::JwksLimits::default())
                .expect("registry");
        registry.install_keys("k8s", &jwks, now).expect("keys");
        let kinds = [(
            "k8s".to_string(),
            coord_authn::WorkloadKind::Kubernetes(coord_authn::KubernetesMode::Offline),
        )]
        .into_iter()
        .collect();
        let verifier = coord_authn::WifVerifier::new(registry, kinds);
        let signer =
            coord_node_issuer::Ca::load(ca.certificate.der(), &ca.key.serialize_der(), now)
                .expect("the domain's CA is a usable issuer CA");
        let rules = nodes
            .iter()
            .flat_map(|n| {
                [
                    coord_types::wire_v1::PeerRole::Voter,
                    coord_types::wire_v1::PeerRole::Frontend,
                ]
                .map(|role| (n, role))
            })
            .map(|(n, role)| coord_node_issuer::RolePolicy {
                issuer: "k8s".into(),
                required: [
                    ("namespace".to_string(), "voters".to_string()),
                    ("serviceaccount".to_string(), format!("voter-{n}")),
                ]
                .into_iter()
                .collect(),
                cluster: coord_types::ids::ClusterId(CLUSTER),
                nodes: vec![coord_types::ids::ReplicaId([*n; 16])],
                role,
                min_incarnation: 1,
                max_lifetime_secs: 3600,
                dns_names: vec![SERVER_NAME.into()],
                ip_addresses: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
            })
            .collect();
        let state = coord_node_issuer::IssuerState::new(
            coord_node_issuer::NodeIssuer::new(verifier, signer, rules),
            Box::new(coord_node_issuer::SystemSignClock { uncertainty: 1 }),
            4,
            64 * 1024,
        );
        NodeIssuerFixture {
            state: std::sync::Arc::new(state),
            idp,
        }
    }

    /// Serve on `port` from now on, until the test ends.
    async fn serve(&self, port: u16) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("the issuer's port is free");
        let router = coord_node_issuer::router(std::sync::Arc::clone(&self.state));
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
    }

    /// Write the assertion workload `voters/voter-<n>` presents.
    fn assertion(&self, path: &Path, n: u8) {
        let now = unix_now();
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some("kk".into());
        let claims = serde_json::json!({
            "iss": K8S_ISS,
            "sub": format!("system:serviceaccount:voters:voter-{n}"),
            "aud": ENROLL_AUD,
            "iat": now,
            "exp": now + 900,
            "kubernetes.io": {"namespace": "voters", "serviceaccount": {"name": format!("voter-{n}")}},
        });
        let token = jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_ec_der(&self.idp.serialize_der()),
        )
        .expect("assertion");
        std::fs::write(path, token).expect("write assertion");
    }
}

/// Give `config` a renewal section enrolling at a loopback issuer on
/// `port`, with no spread, so the leaf is due at exactly two thirds of
/// its life.
fn renew_at(config: &Path, port: u16, assertion: &Path) {
    let mut text = std::fs::read_to_string(config).expect("read config");
    text.push_str(&format!(
        "\n[renewal]\n\
         issuer = \"http://127.0.0.1:{port}\"\n\
         assertion = \"{}\"\n\
         lifetime_secs = 120\n\
         jitter_secs = 0\n\
         allow_insecure_loopback = true\n",
        assertion.display()
    ));
    std::fs::write(config, text).expect("write config");
}

/// Replace the collector leaf in `dir` with one that ends `secs` from
/// now, under the collector's own key; its `notAfter`.
fn short_collector(dir: &Path, ca: &Ca, replica: u8, secs: u64) -> u64 {
    use rustls_pki_types::pem::PemObject;
    let key = rustls_pki_types::PrivateKeyDer::from_pem_file(dir.join("collector.key"))
        .expect("the collector key");
    let key = rcgen::KeyPair::try_from(&key).expect("a usable collector key");
    let (leaf, not_after) = ca.issue_short_as(
        &key,
        replica,
        secs,
        coord_types::wire_v1::PeerRole::Frontend,
    );
    std::fs::write(dir.join("collector.pem"), pem("CERTIFICATE", &leaf)).expect("collector cert");
    not_after
}

/// The role a leaf on disk names.
fn role_on_disk(path: &Path) -> coord_types::wire_v1::PeerRole {
    use rustls_pki_types::pem::PemObject;
    let der = rustls_pki_types::CertificateDer::pem_file_iter(path)
        .expect("readable")
        .next()
        .expect("a certificate")
        .expect("pem");
    let (_, x509) = x509_parser::parse_x509_certificate(&der).expect("x509");
    x509.subject_alternative_name()
        .expect("sans")
        .expect("sans")
        .value
        .general_names
        .iter()
        .find_map(|name| match name {
            x509_parser::extensions::GeneralName::URI(uri) => {
                coord_node_issuer::parse_node_uri(uri).map(|id| id.role)
            }
            _ => None,
        })
        .expect("a node identity")
}

/// A single-voter node holding a leaf that ends `secs` from now, and the
/// domain's CA, key ring and the leaf's `notAfter`.
fn short_lived_voter(dir: &Path, secs: u64) -> (Ca, coord_sts::KeyRing, PathBuf, u64) {
    let ca = credentials(dir, 1, coord_types::wire_v1::PeerRole::Voter);
    let key = rcgen::KeyPair::try_from(ca.node_key.as_slice()).expect("node key");
    let (leaf, not_after) = ca.issue_short(&key, 1, secs);
    std::fs::write(dir.join("node.pem"), pem("CERTIFICATE", &leaf)).expect("cert");
    genesis_of(dir, 1, Some(&ca.node_spki));
    let ring = sts_keys(dir);
    let path = config_only(dir);
    (ca, ring, path, not_after)
}

/// The leaf at `path`: its SubjectPublicKeyInfo and `notAfter`.
fn leaf_on_disk(path: &Path) -> (Vec<u8>, u64) {
    use rustls_pki_types::pem::PemObject;
    let der = rustls_pki_types::CertificateDer::pem_file_iter(path)
        .expect("readable")
        .next()
        .expect("a certificate")
        .expect("pem");
    let (_, x509) = x509_parser::parse_x509_certificate(&der).expect("x509");
    (
        x509.public_key().raw.to_vec(),
        x509.validity().not_after.timestamp() as u64,
    )
}

/// Sleep until the wall clock is past `unix`.
async fn until_past(unix: u64) {
    while unix_now() <= unix {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A node whose leaf falls due renews it in place and keeps serving
/// (task-d02): no restart, and no connection dropped for the renewal.
///
/// The issuer is down at the due point and comes back before the
/// deadline, so this is also the outage the early due point is for: the
/// node reports the failed attempt, retries, and renews when the issuer
/// answers. The renewed leaf is on disk under the same key with a later
/// `notAfter`, so a restart would come back on it. A caller that bound
/// before the renewal was authenticated under the old leaf, and the node
/// ends that connection at the old leaf's `notAfter` -- no connection
/// outlives the leaf this end presented on it -- while a caller arriving
/// after that instant connects and is served under the renewed one,
/// which it could not be by a node still presenting the old leaf.
#[tokio::test(flavor = "multi_thread")]
async fn a_leaf_that_falls_due_is_renewed_in_place_and_serving_carries_on() {
    let dir = workspace("renew");
    let (ca, ring, path, old_not_after) = short_lived_voter(&dir, 30);
    let port = free_port();
    let issuer = NodeIssuerFixture::new(&ca, &[1]);
    issuer.assertion(&dir.join("assertion.jwt"), 1);
    renew_at(&path, port, &dir.join("assertion.jwt"));
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let (spki_before, _) = leaf_on_disk(&dir.join("node.pem"));

    let daemon = start(&path);
    let report = daemon.out.lock().expect("not poisoned").clone();
    assert!(
        report.contains("renewal configured")
            && report.contains(&format!("expires_at={old_not_after}"))
            && report.contains("state=wait"),
        "the startup report does not say when this leaf is renewed:\n{report}"
    );
    let early = Caller::bind(&daemon, &ca, &ring, [0x51; 16]).await;

    // The issuer is not there when the leaf falls due.
    assert!(
        daemon.waits_until(40, |said| said.contains("renewal attempt failed")),
        "no attempt was made at the due point:\n{}",
        daemon.said()
    );
    assert!(
        unix_now() < old_not_after,
        "the first attempt came only at the deadline"
    );
    issuer.serve(port).await;
    assert!(
        daemon.waits_until(20, |said| said.contains("renewal renewed expires_at")),
        "the node did not renew once the issuer was back:\n{}",
        daemon.said()
    );
    // Written where a restart reads it: the same key, a later end.
    let (spki_after, new_not_after) = leaf_on_disk(&dir.join("node.pem"));
    assert_eq!(spki_after, spki_before, "the renewal changed the key");
    assert!(
        new_not_after > old_not_after,
        "{new_not_after} <= {old_not_after}"
    );

    // Served before the old leaf's end...
    let answer = ask(&early.connection, &early.put(1, b"k", b"v"))
        .await
        .expect("a caller connected before the renewal is served inside the old leaf's life");
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));
    // ...and closed at it: that connection was authenticated under the
    // old leaf, and nothing is carried under a leaf past its end.
    until_past(old_not_after + 1).await;
    match early.connection.close_reason() {
        Some(quinn::ConnectionError::ApplicationClosed(close)) => assert_eq!(
            close.error_code,
            quinn::VarInt::from_u32(coord_transport::CloseCode::Expired as u32),
            "closed for something other than the leaf's end"
        ),
        other => panic!(
            "a connection outlived the leaf it was authenticated under: {other:?}\n{}",
            daemon.said()
        ),
    }
    // A caller arriving now connects under the renewed leaf, which it
    // could not to a node still presenting the old one.
    let late = Caller::bind(&daemon, &ca, &ring, [0x52; 16]).await;
    let answer = ask(&late.connection, &late.range(1, b"k"))
        .await
        .expect("a caller arriving after the old leaf's end is served");
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));
    assert!(
        !daemon.said().contains("renewal expired"),
        "{}",
        daemon.said()
    );
}

/// With the issuer down from the due point, the node serves to its
/// deadline and stops there (task-d02). It never serves on the expired
/// leaf, and the issuer coming back afterwards does not revive it: a
/// restart on the expired leaf is refused before anything is opened.
#[tokio::test(flavor = "multi_thread")]
async fn with_the_issuer_down_a_node_serves_to_its_deadline_and_stops_there() {
    let dir = workspace("renew-outage");
    let (ca, ring, path, not_after) = short_lived_voter(&dir, 18);
    let port = free_port();
    let issuer = NodeIssuerFixture::new(&ca, &[1]);
    issuer.assertion(&dir.join("assertion.jwt"), 1);
    renew_at(&path, port, &dir.join("assertion.jwt"));
    assert_eq!(run(&path, &["init"]).code, Some(0));

    let mut daemon = start(&path);
    let caller = Caller::bind(&daemon, &ca, &ring, [0x53; 16]).await;
    assert!(
        daemon.waits_until(30, |said| said.contains("renewal attempt failed")),
        "no attempt was made at the due point:\n{}",
        daemon.said()
    );
    // Due, and not renewed: still valid, so still serving.
    assert!(unix_now() < not_after);
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .expect("a node inside its leaf's life serves through an outage");
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));

    // And at the deadline it stops.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = daemon.child.try_wait().expect("wait") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the node kept serving past its leaf's end:\n{}",
            daemon.said()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        unix_now() >= not_after,
        "the node stopped before its leaf's end"
    );
    assert_eq!(status.code(), Some(2), "{}", daemon.said());
    // The reader threads have the last lines once the pipe closes.
    assert!(
        daemon.waits_until(5, |said| said.contains("reason=credential-expired")),
        "{}",
        daemon.said()
    );
    let said = daemon.said();
    assert!(said.contains("renewal expired"), "{said}");
    assert!(said.contains("phase=quarantined"), "{said}");
    assert!(!said.contains("renewal renewed expires_at"), "{said}");
    assert!(said.contains("renewal renewed=0 "), "{said}");

    // The issuer comes back. Nothing revives the node by itself: a
    // restart on the expired leaf is refused before a store is opened.
    // (Past the second `notAfter` names: X.509 counts that second as
    // valid, while the node stops serving at its start.)
    issuer.serve(port).await;
    until_past(not_after + 1).await;
    let restarted = run(&path, &[]);
    assert_eq!(
        restarted.code,
        Some(2),
        "{}{}",
        restarted.out,
        restarted.err
    );
    assert!(
        restarted.err.contains("trust bundle"),
        "the refusal did not say the leaf is not valid: {}",
        restarted.err
    );
}

/// A node that does not renew stops at its leaf's end the same way one
/// that renews does (task-d02; design Sections 10.4, 20.4): the same
/// exit status and the same quarantine reason, so an operator watches for
/// one signal whatever the configuration. Before it stops, its transport
/// has already ended the connection it accepted under that leaf.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_does_not_renew_stops_at_its_leafs_end_like_one_that_does() {
    let dir = workspace("no-renewal");
    let (ca, ring, path, not_after) = short_lived_voter(&dir, 12);
    let init = run(&path, &["init"]);
    assert_eq!(init.code, Some(0), "{}{}", init.out, init.err);

    let mut daemon = start(&path);
    let report = daemon.out.lock().expect("not poisoned").clone();
    assert!(
        report.contains("renewal not-configured")
            && report.contains(&format!("expires_at={not_after}")),
        "the startup report does not say when this leaf ends:\n{report}"
    );
    let caller = Caller::bind(&daemon, &ca, &ring, [0x54; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .expect("a node inside its leaf's life serves");
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = daemon.child.try_wait().expect("wait") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a node without renewal kept serving past its leaf's end:\n{}",
            daemon.said()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        unix_now() >= not_after,
        "the node stopped before its leaf's end"
    );
    assert_eq!(status.code(), Some(2), "{}", daemon.said());
    assert!(
        daemon.waits_until(5, |said| said.contains("reason=credential-expired")),
        "{}",
        daemon.said()
    );
    let said = daemon.said();
    assert!(said.contains("phase=quarantined"), "{said}");
    assert!(
        said.contains(&format!("renewal not-configured expires_at={not_after}:")),
        "{said}"
    );
    // The caller's connection went with the leaf, not with the process:
    // it was closed as expired, which a process that merely exited would
    // not have said.
    match caller.connection.close_reason() {
        Some(quinn::ConnectionError::ApplicationClosed(close)) => assert_eq!(
            close.error_code,
            quinn::VarInt::from_u32(coord_transport::CloseCode::Expired as u32)
        ),
        other => panic!("the caller's connection ended for {other:?}"),
    }
}

/// A collector's leaf is renewed like the node's, under the same
/// `[renewal]` section and its own key, and what it submits keeps going
/// past the old leaf's end (task-d02).
///
/// Voter 1's collector holds a short leaf; its node leaf is long. The
/// startup report and `inspect` name both leaves. The collector leaf is
/// renewed as `Frontend` -- the issuer holds a voter rule for the same
/// workload ahead of it, so the request has to say which it is -- and
/// written where a restart reads it, the node's left as it was. At the old
/// collector leaf's end, the links it was presented on close, and voter 1
/// dials them again under the renewed one: `voters submittable=` falls
/// and comes back to 2. Had the renewed leaf not been put into service,
/// voter 1 would dial nothing under the expired one and it would not.
#[tokio::test(flavor = "multi_thread")]
async fn a_collectors_leaf_is_renewed_and_its_links_come_back_under_it() {
    let dir = workspace("renew-collector");
    let cluster = three_voters(&dir);
    let n1 = dir.join("n1");
    let old_not_after = short_collector(&n1, &cluster.ca, 1, 36);
    let (node_spki, node_not_after) = leaf_on_disk(&n1.join("node.pem"));
    let (collector_spki, _) = leaf_on_disk(&n1.join("collector.pem"));
    let port = free_port();
    let issuer = NodeIssuerFixture::new(&cluster.ca, &[1]);
    issuer.assertion(&n1.join("assertion.jwt"), 1);
    renew_at(&cluster.configs[0], port, &n1.join("assertion.jwt"));
    issuer.serve(port).await;

    let inspected = run(&cluster.configs[0], &["inspect"]);
    assert_eq!(inspected.code, Some(0), "{}", inspected.err);
    assert!(
        inspected.out.contains("collector leaf issued_at=")
            && inspected
                .out
                .contains(&format!("expires_at={old_not_after} "))
            && inspected.out.contains("driver=configured"),
        "inspect does not report the collector leaf:\n{}",
        inspected.out
    );
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0));
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    let report = running[0].out.lock().expect("not poisoned").clone();
    assert!(
        report
            .lines()
            .any(|l| l.starts_with("renewal configured issued_at="))
            && report.contains("collector renewal configured issued_at=")
            && report.contains(&format!("expires_at={old_not_after} ")),
        "the startup report does not name both leaves:\n{report}"
    );
    assert!(
        running[0].waits_until(40, |said| said
            .contains("collector renewal renewed expires_at")),
        "the collector leaf was not renewed:\n{}",
        running[0].said()
    );
    // Written where a restart reads it: the collector's key, a later end,
    // still the collector's role; the node's leaf untouched.
    let (spki_after, new_not_after) = leaf_on_disk(&n1.join("collector.pem"));
    assert_eq!(
        spki_after, collector_spki,
        "the renewal changed the collector key"
    );
    assert!(
        new_not_after > old_not_after,
        "{new_not_after} <= {old_not_after}"
    );
    assert_eq!(
        role_on_disk(&n1.join("collector.pem")),
        coord_types::wire_v1::PeerRole::Frontend
    );
    assert_eq!(
        leaf_on_disk(&n1.join("node.pem")),
        (node_spki, node_not_after),
        "the collector's renewal touched the node's leaf"
    );

    // Past the old collector leaf's end, and then some: X.509 counts the
    // `notAfter` second as valid, so a count read at the boundary proves
    // nothing.
    until_past(old_not_after + 2).await;
    assert!(
        running[0].waits_until(60, |said| healed(said, "voters submittable=", 2)),
        "voter 1's collector links did not come back under the renewed leaf:\n{}",
        running[0].said()
    );
    let said = running[0].said();
    assert!(!said.contains("collector renewal expired"), "{said}");
    assert!(!said.contains("reason=credential-expired"), "{said}");
}

/// A collector leaf that nothing renews ends the node at its `notAfter`,
/// the same way the node's own leaf does (task-d02): a frontend whose
/// collector can no longer reach another voter is half a node.
#[tokio::test(flavor = "multi_thread")]
async fn a_collector_leaf_nothing_renews_ends_the_node_at_its_end() {
    let dir = workspace("collector-expiry");
    let (ca, _ring, path, node_not_after) = short_lived_voter(&dir, 3000);
    let not_after = short_collector(&dir, &ca, 1, 12);
    assert_eq!(run(&path, &["init"]).code, Some(0));
    let mut daemon = start(&path);
    let report = daemon.out.lock().expect("not poisoned").clone();
    assert!(
        report.contains("collector renewal not-configured")
            && report.contains(&format!("expires_at={not_after}")),
        "the startup report does not say when the collector leaf ends:\n{report}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = daemon.child.try_wait().expect("wait") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the node kept serving past its collector leaf's end:\n{}",
            daemon.said()
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        unix_now() >= not_after,
        "the node stopped before the collector leaf's end"
    );
    assert!(unix_now() < node_not_after);
    assert_eq!(status.code(), Some(2), "{}", daemon.said());
    assert!(
        daemon.waits_until(5, |said| said.contains("reason=credential-expired")),
        "{}",
        daemon.said()
    );
    assert!(
        daemon.said().contains(&format!(
            "collector renewal not-configured expires_at={not_after}: this node's collector leaf"
        )),
        "{}",
        daemon.said()
    );
}

/// The renewed leaf of one voter of three is admitted by its peers as a
/// renewal (task-d02): once the old leaf's end has closed every link it
/// authenticated, the peers dial and accept the node again under the new
/// one, and a request through it -- it leads, so nothing is established
/// without its links -- is established by the three.
#[tokio::test(flavor = "multi_thread")]
async fn peers_admit_a_voters_renewed_leaf() {
    let dir = workspace("renew-peers");
    let cluster = three_voters(&dir);
    let n1 = dir.join("n1");
    let key = rcgen::KeyPair::try_from(cluster.ca.node_key.as_slice()).expect("voter 1 key");
    let (leaf, old_not_after) = cluster.ca.issue_short(&key, 1, 36);
    std::fs::write(n1.join("node.pem"), pem("CERTIFICATE", &leaf)).expect("cert");
    let port = free_port();
    let issuer = NodeIssuerFixture::new(&cluster.ca, &[1]);
    issuer.assertion(&n1.join("assertion.jwt"), 1);
    renew_at(&cluster.configs[0], port, &n1.join("assertion.jwt"));
    issuer.serve(port).await;
    for config in &cluster.configs {
        assert_eq!(run(config, &["init"]).code, Some(0));
    }
    let running: Vec<Running> = cluster.configs.iter().map(|c| start(c)).collect();
    for node in &running {
        assert!(
            node.waits_to_say("peers connected=2 of 2"),
            "{}",
            node.said()
        );
    }
    assert!(
        running[0].waits_until(40, |said| said.contains("renewal renewed expires_at")),
        "voter 1 did not renew:\n{}",
        running[0].said()
    );
    // The old leaf's end closes the links it authenticated -- each peer
    // holds that deadline for voter 1's connections, and may act on it up
    // to a second early -- and the peers get them back under the renewed
    // one. What each says from just before that instant has to show the
    // link going and coming back.
    //
    // Checked from two seconds past it. A link dialled again within the
    // second `notAfter` names is still accepted under the old leaf --
    // X.509 counts that second as valid -- and is closed again at once,
    // so a count read at the boundary can say 2 for a node its peers are
    // about to refuse.
    until_past(old_not_after - 3).await;
    let marks: Vec<usize> = running.iter().map(|node| node.said().len()).collect();
    until_past(old_not_after + 2).await;
    let returned = |said: &str| {
        let counts = reported(said, "peers connected=");
        counts
            .iter()
            .position(|n| *n < 2)
            .is_some_and(|down| counts[down..].contains(&2))
            && counts.last() == Some(&2)
    };
    for (n, node) in running.iter().enumerate().skip(1) {
        assert!(
            node.waits_until(30, |said| returned(&said[marks[n]..])),
            "voter {} did not take voter 1 back under its renewed leaf:\n{}",
            n + 1,
            &node.said()[marks[n]..]
        );
        assert!(
            !node.said()[marks[n]..].contains("Rejected(Rejected("),
            "voter {} refused voter 1's renewed leaf:\n{}",
            n + 1,
            node.said()
        );
    }
    // And they stay back. A link that is refused keeps being dialled, and
    // each dial that the old leaf's last second let through is closed at
    // once; a mesh that has really taken the node back says nothing more.
    let settled: Vec<usize> = running.iter().map(|node| node.said().len()).collect();
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (n, node) in running.iter().enumerate().skip(1) {
        let said = node.said();
        assert!(
            reported(&said[settled[n]..], "peers connected=").is_empty()
                && reported(&said[marks[n]..], "peers connected=").last() == Some(&2),
            "voter {}'s link to voter 1 did not hold:\n{}",
            n + 1,
            &said[marks[n]..]
        );
    }
    let caller = Caller::bind(&running[0], &cluster.ca, &cluster.ring, [0x54; 16]).await;
    let answer = ask(&caller.connection, &caller.put(1, b"k", b"v"))
        .await
        .unwrap_or_else(|| {
            panic!(
                "the renewed voter's request was not established\n-- 1 --\n{}\n-- 2 --\n{}\n-- 3 --\n{}",
                running[0].said(),
                running[1].said(),
                running[2].said()
            )
        });
    assert!(matches!(
        response_of(&answer).outcome,
        coord_types::wire_v1::OutcomeV1::Ok { .. }
    ));
    for node in &running {
        assert!(!node.said().contains("renewal expired"), "{}", node.said());
    }
}
