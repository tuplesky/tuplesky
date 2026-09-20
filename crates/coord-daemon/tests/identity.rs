//! Acceptance for loading this node's credentials (task-43).
//!
//! Each case here is one a process could otherwise start with and then
//! fail at its first handshake, where the failure reads as a peer
//! problem rather than as this node's own configuration.

use std::io::Write;
use std::path::PathBuf;

use coord_daemon::config::IdentityConfig;
use coord_daemon::identity::{IdentityError, load};
use coord_transport::Class;
use coord_types::ids::{ClusterId, DomainId};

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn dir() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "coord-identity-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&path).expect("temp dir");
    path
}

fn write(dir: &std::path::Path, name: &str, bytes: &[u8], mode: u32) -> String {
    let path = dir.join(name);
    // A test writes the same name more than once under different modes,
    // and one of those modes is read-only. Truncating a read-only file in
    // place is refused for anyone but root, so the old one goes first.
    let _ = std::fs::remove_file(&path);
    let mut file = std::fs::File::create(&path).expect("create");
    file.write_all(bytes).expect("write");
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }
    let _ = mode;
    path.to_string_lossy().into_owned()
}

fn pem(label: &str, der: &[u8]) -> Vec<u8> {
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
    out.into_bytes()
}

/// The error of a load, or `None` if it succeeded.
///
/// `LocalIdentity` is deliberately neither `Debug` nor `PartialEq`: it
/// holds a private key, and a type that could print or compare one is a
/// type that will eventually do so in a log line. So a test states what
/// it means -- which refusal came back -- rather than comparing results.
fn refusal(config: &IdentityConfig) -> Option<IdentityError> {
    load(config, CLUSTER, DOMAIN, Vec::new(), Class::Peer, None).err()
}

/// A real certificate and its key, so a good case is actually good
/// rather than merely well-shaped.
fn credentials() -> (Vec<u8>, Vec<u8>) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut params = rcgen::CertificateParams::new(vec!["node.local".into()]).expect("params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let certificate = params.self_signed(&key).expect("self-signed");
    (
        pem("CERTIFICATE", certificate.der()),
        pem("PRIVATE KEY", &key.serialize_der()),
    )
}

fn config(dir: &std::path::Path, key_mode: u32) -> IdentityConfig {
    let (certificate, key) = credentials();
    IdentityConfig {
        trust_bundle: write(dir, "roots.pem", &certificate, 0o644),
        node_certificate: write(dir, "node.pem", &certificate, 0o644),
        node_key: write(dir, "node.key", &key, key_mode),
        collector_certificate: None,
        collector_key: None,
    }
}

/// The ordinary case: three readable files, a key only this account can
/// read, and an identity that names the cluster and domain it was given.
#[test]
fn credentials_this_account_holds_are_loaded() {
    let dir = dir();
    let identity = load(
        &config(&dir, 0o600),
        CLUSTER,
        DOMAIN,
        vec![0x0011],
        Class::Peer,
        None,
    )
    .expect("the credentials load");
    assert_eq!(identity.cluster, CLUSTER);
    assert_eq!(identity.domain, DOMAIN);
    assert_eq!(identity.capabilities, vec![0x0011]);
    assert_eq!(identity.chain.len(), 1, "the chain is what was presented");
    assert!(!identity.roots.is_empty(), "the roots are what was trusted");
}

/// A process that is more than one principal presents more than one
/// credential.
///
/// A node certificate binds exactly one role, and a process that runs a
/// voter and that domain's collector acts as both. The collector's
/// credential is what it presents when it dials another voter to submit
/// on a client's behalf; what it *serves* as is unchanged, which is why
/// this is a separate chain rather than a replacement for the node's.
#[test]
fn a_collector_credential_is_loaded_beside_the_nodes() {
    let dir = dir();
    let (certificate, key) = credentials();
    let mut both = config(&dir, 0o600);
    both.collector_certificate = Some(write(&dir, "collector.pem", &certificate, 0o644));
    both.collector_key = Some(write(&dir, "collector.key", &key, 0o600));

    let identity = load(&both, CLUSTER, DOMAIN, Vec::new(), Class::Api, None).expect("both load");

    assert!(
        identity.api_client.is_some(),
        "the collector credential was named and not loaded"
    );
    assert!(
        load(
            &config(&dir, 0o600),
            CLUSTER,
            DOMAIN,
            Vec::new(),
            Class::Api,
            None
        )
        .expect("the node alone loads")
        .api_client
        .is_none(),
        "a process that named no collector credential acquired one"
    );
}

/// Half a credential is not a credential.
///
/// A certificate with no key cannot be presented and a key with no
/// certificate names nobody. Either way the process would fall back to
/// presenting the node's certificate -- a different principal, with a
/// different role -- so it is refused at startup instead.
#[test]
fn half_a_collector_credential_is_refused() {
    let dir = dir();
    let (certificate, key) = credentials();

    let mut certificate_only = config(&dir, 0o600);
    certificate_only.collector_certificate = Some(write(&dir, "c.pem", &certificate, 0o644));
    assert_eq!(
        refusal(&certificate_only),
        Some(IdentityError::HalfACollectorCredential)
    );

    let mut key_only = config(&dir, 0o600);
    key_only.collector_key = Some(write(&dir, "c.key", &key, 0o600));
    assert_eq!(
        refusal(&key_only),
        Some(IdentityError::HalfACollectorCredential)
    );
}

/// The collector's key is held to the same rule as the node's: a key
/// other accounts can read is a key that has already left this process.
#[test]
#[cfg(unix)]
fn a_collector_key_other_accounts_can_read_is_refused() {
    let dir = dir();
    let (certificate, key) = credentials();
    let mut shared = config(&dir, 0o600);
    shared.collector_certificate = Some(write(&dir, "collector.pem", &certificate, 0o644));
    let path = write(&dir, "collector.key", &key, 0o644);
    shared.collector_key = Some(path.clone());

    assert_eq!(
        refusal(&shared),
        Some(IdentityError::KeyIsShared { path, mode: 0o644 })
    );
}

/// A private key other accounts can read has already left this process's
/// control. Reading it and carrying on would make the configuration's
/// promise untrue and unremarked, so it is refused -- and the mode is
/// reported, because the operator's next step is to change it.
#[test]
#[cfg(unix)]
fn a_private_key_other_accounts_can_read_is_refused() {
    let dir = dir();
    for mode in [0o644, 0o640, 0o604, 0o666] {
        let shared = config(&dir, mode);
        assert_eq!(
            refusal(&shared),
            Some(IdentityError::KeyIsShared {
                path: shared.node_key.clone(),
                mode,
            }),
            "mode {mode:o} was accepted"
        );
    }
    // The key itself is never in the message: the path and the mode are,
    // and neither is material.
    let shared = config(&dir, 0o644);
    let message = refusal(&shared).expect("refused").to_string();
    assert!(message.contains("node.key") && message.contains("644"));
    assert!(!message.contains("PRIVATE KEY") && !message.contains("BEGIN"));

    // Modes that keep the key to this account are accepted.
    for mode in [0o600, 0o400, 0o700] {
        assert_eq!(
            refusal(&config(&dir, mode)),
            None,
            "mode {mode:o} was refused"
        );
    }
}

/// A trust bundle with no certificates is an empty root store, which
/// trusts nothing: a process that started with one would accept no peer
/// and report itself healthy while doing so.
#[test]
fn a_trust_bundle_that_trusts_nothing_is_refused() {
    let dir = dir();
    let mut config = config(&dir, 0o600);
    config.trust_bundle = write(&dir, "empty-roots.pem", b"# nothing here\n", 0o644);
    assert_eq!(
        refusal(&config),
        Some(IdentityError::Empty {
            what: "trust bundle",
            path: config.trust_bundle.clone(),
        })
    );
}

/// A chain with nothing to present fails every handshake, with no
/// indication that the cause is this node's own configuration.
#[test]
fn a_certificate_chain_with_nothing_in_it_is_refused() {
    let dir = dir();
    let mut config = config(&dir, 0o600);
    config.node_certificate = write(&dir, "empty-node.pem", b"", 0o644);
    assert_eq!(
        refusal(&config),
        Some(IdentityError::Empty {
            what: "node certificate",
            path: config.node_certificate.clone(),
        })
    );
}

/// A file that is missing and a file that is the wrong kind are two
/// different problems with two different next steps, and are reported as
/// such rather than as one "bad credentials".
#[test]
fn a_missing_file_and_a_wrong_one_are_told_apart() {
    let dir = dir();

    let mut missing = config(&dir, 0o600);
    missing.trust_bundle = dir.join("absent.pem").to_string_lossy().into_owned();
    assert!(
        matches!(
            refusal(&missing),
            Some(IdentityError::Unreadable {
                what: "trust bundle",
                ..
            })
        ),
        "a missing bundle was not reported as unreadable"
    );

    // A key file that holds a certificate is present, readable and
    // private -- and still not a key.
    let mut wrong = config(&dir, 0o600);
    let (certificate, _) = credentials();
    wrong.node_key = write(&dir, "not-a-key.pem", &certificate, 0o600);
    assert!(
        matches!(
            refusal(&wrong),
            Some(IdentityError::Empty {
                what: "node key",
                ..
            }) | Some(IdentityError::Malformed {
                what: "node key",
                ..
            })
        ),
        "a certificate was accepted as a private key"
    );
}

/// An issuing authority and a leaf it signed, as the node issuer would
/// hand them out: the bundle holds the authority, the node holds the
/// leaf and its key.
fn issued() -> (rcgen::Certificate, rcgen::KeyPair, Vec<u8>, Vec<u8>) {
    let ca_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let params = rcgen::CertificateParams::new(vec!["node.local".into()]).expect("leaf params");
    let leaf = params.signed_by(&key, &issuer).expect("leaf");
    let leaf_pem = pem("CERTIFICATE", leaf.der());
    let key_pem = pem("PRIVATE KEY", &key.serialize_der());
    (ca, key, leaf_pem, key_pem)
}

fn verified(config: &IdentityConfig) -> Result<(), IdentityError> {
    let identity = load(config, CLUSTER, DOMAIN, Vec::new()).expect("the credentials load");
    coord_daemon::identity::verify(&identity, config)
}

/// A certificate is an identity only once the trust bundle vouches for
/// it and this process holds its key. The node's replica is read out of
/// the certificate, so a leaf nothing trusted issued -- or one whose key
/// is somebody else's -- could otherwise claim to be any voter and open
/// that voter's store.
#[test]
fn a_certificate_is_an_identity_only_if_the_bundle_issued_it_and_the_key_is_its_own() {
    let dir = dir();
    let (ca, _, leaf, key) = issued();
    let good = IdentityConfig {
        trust_bundle: write(&dir, "issuer.pem", &pem("CERTIFICATE", ca.der()), 0o644),
        node_certificate: write(&dir, "leaf.pem", &leaf, 0o644),
        node_key: write(&dir, "leaf.key", &key, 0o600),
    };
    assert_eq!(verified(&good), Ok(()));

    // The same shape of leaf, signed by an authority the bundle does not
    // hold.
    let (_, _, stranger, stranger_key) = issued();
    let untrusted = IdentityConfig {
        node_certificate: write(&dir, "stranger.pem", &stranger, 0o644),
        node_key: write(&dir, "stranger.key", &stranger_key, 0o600),
        ..good.clone()
    };
    assert!(
        matches!(
            verified(&untrusted),
            Err(IdentityError::NotIssuedByTrustBundle { .. })
        ),
        "a leaf of an untrusted issuer was accepted"
    );

    // The trusted leaf, presented with a key that is not its own.
    let foreign_key = IdentityConfig {
        node_key: write(&dir, "other.key", &stranger_key, 0o600),
        ..good.clone()
    };
    assert!(
        matches!(
            verified(&foreign_key),
            Err(IdentityError::KeyDoesNotMatchCertificate { .. })
        ),
        "a certificate was accepted with a key it does not certify"
    );
}
