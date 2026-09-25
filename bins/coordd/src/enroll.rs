//! Enrolling at the node issuer for a renewed leaf, and deciding whether
//! what comes back may be presented (task-d02; design Sections 10.2,
//! 10.4, 20.4).
//!
//! A renewal is an enrollment the node already knows the answer to. It
//! presents the workload assertion the issuer verifies and a request
//! signed with the key it already holds, and it expects back a leaf that
//! differs from the one it serves on in exactly one way: it ends later.
//! Everything else -- the node, the incarnation, the role, the key, the
//! names peers reach it by, the authority that signed it -- is checked
//! against the leaf being replaced before anything is written or
//! presented, and a leaf that differs in any of those is refused loudly
//! and the node stays on the one it has.
//!
//! That is what keeps a renewal from being anything else. A different
//! key or incarnation is a committed replacement (task-m03), which peers
//! would refuse as `UncommittedKey` or `RequiresCommit` the moment it
//! was presented; a different role would be an observer or collector
//! acquiring a voter's entitlement by asking for it; a narrower set of
//! names would make the node unreachable at the moment its credential was
//! meant to be extended. Each is refused here, where it is a report,
//! rather than at a peer's handshake, where it is an outage.
//!
//! Nothing secret is printed. The assertion is read from its file at each
//! attempt and sent to the issuer only; failures are reported as bounded
//! classes, and the issuer's error codes are kept only when they are the
//! short codes it is known to send.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coord_membership::CredentialChange;
use coord_membership::membership::Membership;
use coord_node_issuer::{Leaf, NodeIdentity, parse_node_uri};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use x509_parser::prelude::FromDer;

/// The longest assertion file read. A service-account token is a few
/// kilobytes; a file past this is not one.
const MAX_ASSERTION_BYTES: u64 = 64 * 1024;

/// The longest issuer response read.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// A whole enrollment, request to response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Reaching the issuer at all.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The credential this node presents, as renewal needs to know it.
pub struct Credential {
    /// The chain, leaf first.
    pub chain: Vec<CertificateDer<'static>>,
    /// The leaf's key. It never changes across a renewal.
    pub key: Arc<PrivateKeyDer<'static>>,
    /// The roots the leaf chains to.
    pub roots: Arc<RootCertStore>,
    /// Who the leaf says this node is.
    pub identity: NodeIdentity,
    /// The leaf's validity.
    pub leaf: Leaf,
    /// The leaf's SubjectPublicKeyInfo.
    spki: Vec<u8>,
    /// The DNS names and addresses the leaf is valid for.
    names: (BTreeSet<String>, BTreeSet<IpAddr>),
}

impl Credential {
    /// Read what renewal needs out of a chain this node already verified.
    pub fn of(
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        roots: Arc<RootCertStore>,
    ) -> Result<Self, String> {
        let leaf = chain.first().ok_or("the node certificate holds no leaf")?;
        let read = read_leaf(leaf).ok_or("the node certificate carries no node identity")?;
        Ok(Credential {
            chain,
            key: Arc::new(key),
            roots,
            identity: read.identity,
            leaf: read.leaf,
            spki: read.spki,
            names: read.names,
        })
    }
}

/// What a leaf says, as far as renewal compares it.
struct ReadLeaf {
    identity: NodeIdentity,
    leaf: Leaf,
    spki: Vec<u8>,
    names: (BTreeSet<String>, BTreeSet<IpAddr>),
}

fn read_leaf(der: &CertificateDer<'_>) -> Option<ReadLeaf> {
    let (_, x509) = x509_parser::certificate::X509Certificate::from_der(der).ok()?;
    let mut identity = None;
    let mut dns = BTreeSet::new();
    let mut ips = BTreeSet::new();
    if let Ok(Some(san)) = x509.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                x509_parser::extensions::GeneralName::URI(uri) => {
                    if identity.is_none() {
                        identity = parse_node_uri(uri);
                    }
                }
                x509_parser::extensions::GeneralName::DNSName(name) => {
                    dns.insert(name.to_ascii_lowercase());
                }
                x509_parser::extensions::GeneralName::IPAddress(bytes) => {
                    let ip = match bytes.len() {
                        4 => <[u8; 4]>::try_from(*bytes).ok().map(IpAddr::from),
                        16 => <[u8; 16]>::try_from(*bytes).ok().map(IpAddr::from),
                        _ => None,
                    };
                    ips.extend(ip);
                }
                _ => {}
            }
        }
    }
    Some(ReadLeaf {
        identity: identity?,
        leaf: Leaf {
            issued_at: u64::try_from(x509.validity().not_before.timestamp()).unwrap_or(0),
            expires_at: u64::try_from(x509.validity().not_after.timestamp()).unwrap_or(0),
        },
        spki: x509.public_key().raw.to_vec(),
        names: (dns, ips),
    })
}

/// Why an attempt ended without a leaf put into service. Bounded, and
/// never material: no assertion, key or certificate bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The assertion file could not be read.
    Assertion(String),
    /// The request could not be built from this node's key.
    Request,
    /// The issuer could not be reached, or did not answer in time.
    Unreachable(&'static str),
    /// The issuer answered with a refusal.
    Issuer {
        /// The HTTP status.
        status: u16,
        /// The issuer's error code, where it was one of its short codes.
        code: String,
    },
    /// The issuer's answer was not one this node understands.
    Malformed,
    /// The issuer answered with a leaf this node will not present.
    Refused(String),
    /// The renewed leaf could not be written where a restart reads it.
    Install(String),
}

impl core::fmt::Display for Failure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Failure::Assertion(why) => write!(f, "the assertion could not be read: {why}"),
            Failure::Request => f.write_str("no request could be signed with this node's key"),
            Failure::Unreachable(why) => write!(f, "the issuer is unreachable: {why}"),
            Failure::Issuer { status, code } => {
                write!(f, "the issuer refused: status={status} code={code}")
            }
            Failure::Malformed => f.write_str("the issuer's answer is not an enrollment"),
            Failure::Refused(why) => write!(f, "the renewed leaf is refused: {why}"),
            Failure::Install(why) => write!(f, "the renewed leaf could not be written: {why}"),
        }
    }
}

/// What a renewal asks the issuer, and where its answer is kept.
pub struct Enroller {
    client: reqwest::Client,
    /// `{issuer}/enroll`.
    url: String,
    /// The assertion file.
    assertion: PathBuf,
    /// The lifetime asked for.
    lifetime_secs: u64,
    /// Where the node certificate lives, which a renewed chain replaces
    /// so that a restart comes back on it.
    certificate: PathBuf,
    /// The committed configuration a voter's renewed key is classified
    /// against, exactly as its peers classify it.
    membership: Membership,
    /// Whether this node votes.
    votes: bool,
}

impl Enroller {
    /// An enroller for `config`, replacing the chain at `certificate`.
    ///
    /// Built before any listener exists, so a configuration that cannot
    /// renew -- an unreadable root bundle, say -- stops the node at
    /// startup rather than at the due point, hours into serving.
    pub fn new(
        config: &coord_daemon::RenewalConfig,
        certificate: &str,
        membership: Membership,
        votes: bool,
    ) -> Result<Self, String> {
        // The TLS stack is the explicit AWS-LC provider; installing twice
        // is harmless.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        // No redirects, no proxy from the environment, bounded time: the
        // issuer is a configured endpoint, and a node that could be sent
        // somewhere else for its credential could be sent anywhere.
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .https_only(!config.allow_insecure_loopback)
            .user_agent("coordd-renewal/1");
        if let Some(path) = &config.issuer_roots {
            let pem = std::fs::read(path)
                .map_err(|e| format!("cannot read the issuer roots at {path}: {e}"))?;
            let roots = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|_| format!("the issuer roots at {path} are not PEM certificates"))?;
            if roots.is_empty() {
                return Err(format!("the issuer roots at {path} contain none"));
            }
            builder = builder.tls_built_in_root_certs(false);
            for root in roots {
                builder = builder.add_root_certificate(root);
            }
        }
        let client = builder
            .build()
            .map_err(|e| format!("the issuer client could not be built: {e}"))?;
        Ok(Enroller {
            client,
            url: format!("{}/enroll", config.issuer.trim_end_matches('/')),
            assertion: PathBuf::from(&config.assertion),
            lifetime_secs: config.lifetime_secs,
            certificate: PathBuf::from(certificate),
            membership,
            votes,
        })
    }

    /// Enroll for a renewal of `current`, check what comes back, and
    /// write it where a restart reads it. The caller puts it into service.
    pub async fn renew(&self, current: &Credential) -> Result<Credential, Failure> {
        let assertion = read_assertion(&self.assertion)?;
        // Signed with the key the node already holds: this is what makes
        // the answer a renewal of this node's committed key rather than a
        // certificate for a new one.
        let key = rcgen::KeyPair::try_from(&*current.key).map_err(|_| Failure::Request)?;
        let csr = rcgen::CertificateParams::default()
            .serialize_request(&key)
            .map_err(|_| Failure::Request)?;
        let body = serde_json::json!({
            "assertion": assertion,
            "csr": b64url(csr.der()),
            "node": hex(&current.identity.node.0),
            "incarnation": current.identity.incarnation.get(),
            "lifetime_secs": self.lifetime_secs,
        });
        let mut response = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(unreachable)?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(Failure::Malformed);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(unreachable)? {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(Failure::Malformed);
            }
            bytes.extend_from_slice(&chunk);
        }
        let answer: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| Failure::Malformed)?;
        if !status.is_success() {
            return Err(Failure::Issuer {
                status: status.as_u16(),
                code: answer
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .filter(|c| {
                        !c.is_empty()
                            && c.len() <= 32
                            && c.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                    })
                    .unwrap_or("unknown")
                    .to_owned(),
            });
        }
        let leaf = answer
            .get("certificate")
            .and_then(serde_json::Value::as_str)
            .and_then(b64url_decode)
            .ok_or(Failure::Malformed)?;
        let renewed = accept(
            current,
            CertificateDer::from(leaf),
            self.votes.then_some(&self.membership),
        )?;
        install(&self.certificate, &renewed.chain)?;
        Ok(renewed)
    }
}

/// Decide whether `leaf` may replace `current`'s, and build the credential
/// it would be.
///
/// `membership` is the committed configuration where this node votes: a
/// voter's renewed key has to classify as `Renewal` there, the only
/// classification its peers bind.
pub fn accept(
    current: &Credential,
    leaf: CertificateDer<'static>,
    membership: Option<&Membership>,
) -> Result<Credential, Failure> {
    let refused = |why: &str| Failure::Refused(why.to_owned());
    let read = read_leaf(&leaf).ok_or_else(|| refused("it carries no node identity"))?;
    let was = &current.identity;
    let now = &read.identity;
    if now.cluster != was.cluster || now.node != was.node {
        return Err(refused("it names another node"));
    }
    if now.incarnation != was.incarnation {
        return Err(refused(
            "it names another incarnation, which is a committed replacement and not a renewal",
        ));
    }
    if now.role != was.role {
        return Err(refused(
            "it names another role, and a renewal does not change what a node may do",
        ));
    }
    if read.spki != current.spki {
        return Err(refused(
            "it certifies another key, which is a committed replacement and not a renewal",
        ));
    }
    if let Some(membership) = membership {
        let change = membership.classify_credential(&now.node, now.incarnation, &read.spki);
        if change != CredentialChange::Renewal {
            return Err(Failure::Refused(format!(
                "the committed configuration classifies it as {change:?}, not as a renewal"
            )));
        }
    }
    if !current.names.0.is_subset(&read.names.0) || !current.names.1.is_subset(&read.names.1) {
        return Err(refused(
            "it drops a name or address the current leaf is reached by",
        ));
    }
    if read.leaf.expires_at <= current.leaf.expires_at {
        return Err(refused("it does not outlive the leaf it would replace"));
    }
    // The renewed leaf in place of the old one, under the same
    // intermediates; and the same check a start makes of the credential it
    // serves on, so a renewal cannot present a leaf a restart would refuse.
    let mut chain = Vec::with_capacity(current.chain.len());
    chain.push(leaf);
    chain.extend(current.chain.iter().skip(1).cloned());
    coord_daemon::verify_chain(&chain, &current.key, &current.roots)
        .map_err(|e| Failure::Refused(e.to_string()))?;
    Ok(Credential {
        chain,
        key: current.key.clone(),
        roots: current.roots.clone(),
        identity: read.identity,
        leaf: read.leaf,
        spki: read.spki,
        names: read.names,
    })
}

/// Replace the chain at `path` with `chain`, atomically.
///
/// Written beside the file and renamed over it, with the file's own
/// permissions, so a crash leaves either the old chain or the new one and
/// never half of either -- a start reads this file, and a torn one would
/// be a node that cannot come back at all.
fn install(path: &Path, chain: &[CertificateDer<'static>]) -> Result<(), Failure> {
    use std::io::Write;

    let failed = |e: std::io::Error| Failure::Install(e.kind().to_string());
    let name = path
        .file_name()
        .ok_or_else(|| Failure::Install("the certificate path names no file".into()))?;
    let directory = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let staged = directory.join(format!(".{}.renewing", name.to_string_lossy()));
    let permissions = std::fs::metadata(path).map_err(failed)?.permissions();
    let text: String = chain.iter().map(|c| pem("CERTIFICATE", c)).collect();
    let written = (|| {
        let mut file = std::fs::File::create(&staged)?;
        file.set_permissions(permissions)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&staged, path)?;
        // The rename is durable once the directory is.
        #[cfg(unix)]
        std::fs::File::open(&directory)?.sync_all()?;
        Ok(())
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&staged);
        return Err(failed(e));
    }
    Ok(())
}

/// The assertion, read fresh: the platform rotates the file in place.
fn read_assertion(path: &Path) -> Result<String, Failure> {
    use std::io::Read;

    let file = std::fs::File::open(path).map_err(|e| Failure::Assertion(e.kind().to_string()))?;
    let mut text = String::new();
    file.take(MAX_ASSERTION_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| Failure::Assertion(e.kind().to_string()))?;
    if text.len() as u64 > MAX_ASSERTION_BYTES {
        return Err(Failure::Assertion("larger than any assertion".into()));
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(Failure::Assertion("empty".into()));
    }
    Ok(trimmed.to_owned())
}

/// A transport failure as a bounded class. The error itself can carry
/// the URL and more; none of it is needed to act on.
fn unreachable(e: reqwest::Error) -> Failure {
    Failure::Unreachable(if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "no connection"
    } else if e.is_redirect() {
        "redirected"
    } else {
        "transport"
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Base64 of `bytes`, padded or url-safe unpadded.
fn base64(bytes: &[u8], url: bool) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                let c = B64[((n >> (18 - 6 * i)) & 0x3f) as usize];
                out.push(match (url, c) {
                    (true, b'+') => '-',
                    (true, b'/') => '_',
                    _ => c as char,
                });
            } else if !url {
                out.push('=');
            }
        }
    }
    out
}

fn b64url(bytes: &[u8]) -> String {
    base64(bytes, true)
}

fn b64url_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn pem(label: &str, der: &[u8]) -> String {
    let body = base64(der, false);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(core::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_types::wire_v1::PeerRole;
    use rustls_pki_types::pem::PemObject;

    const CLUSTER: [u8; 16] = [0x11; 16];

    struct Ca {
        issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
        der: CertificateDer<'static>,
    }

    fn new_ca() -> Ca {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let der = params.self_signed(&key).unwrap().der().clone();
        Ca {
            issuer: rcgen::Issuer::new(params, key),
            der,
        }
    }

    struct Leafs<'a> {
        ca: &'a Ca,
        key: rcgen::KeyPair,
    }

    impl Leafs<'_> {
        /// A leaf for `key`, naming `role` at `incarnation`, valid for
        /// `secs` from now and for `names`.
        fn issue(
            &self,
            key: &rcgen::KeyPair,
            incarnation: u64,
            role: PeerRole,
            secs: i64,
            names: &[&str],
        ) -> CertificateDer<'static> {
            let identity = NodeIdentity {
                cluster: coord_types::ids::ClusterId(CLUSTER),
                node: coord_types::ids::ReplicaId([1; 16]),
                incarnation: coord_types::ids::ReplicaIncarnation::new(incarnation).unwrap(),
                role,
            };
            let mut params = rcgen::CertificateParams::default();
            let now = time::OffsetDateTime::now_utc();
            params.not_before = now - time::Duration::seconds(5);
            params.not_after = now + time::Duration::seconds(secs);
            params.extended_key_usages = vec![
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let mut sans: Vec<rcgen::SanType> = names
                .iter()
                .map(|n| rcgen::SanType::DnsName((*n).try_into().unwrap()))
                .collect();
            sans.push(rcgen::SanType::URI(
                coord_node_issuer::node_uri(&identity).try_into().unwrap(),
            ));
            params.subject_alt_names = sans;
            params
                .signed_by(key, &self.ca.issuer)
                .unwrap()
                .der()
                .clone()
        }

        fn current(&self, role: PeerRole) -> Credential {
            let leaf = self.issue(&self.key, 1, role, 60, &["node.test"]);
            let mut roots = RootCertStore::empty();
            roots.add(self.ca.der.clone()).unwrap();
            Credential::of(
                vec![leaf],
                PrivateKeyDer::from_pem_slice(
                    pem("PRIVATE KEY", &self.key.serialize_der()).as_bytes(),
                )
                .unwrap(),
                Arc::new(roots),
            )
            .unwrap()
        }
    }

    fn fixture(ca: &Ca) -> Leafs<'_> {
        Leafs {
            ca,
            key: rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap(),
        }
    }

    fn refused(result: Result<Credential, Failure>) -> String {
        match result {
            Err(Failure::Refused(why)) => why,
            Err(other) => panic!("refused for the wrong reason: {other}"),
            Ok(_) => panic!("accepted"),
        }
    }

    #[test]
    fn the_same_key_and_identity_ending_later_is_a_renewal() {
        let ca = new_ca();
        let f = fixture(&ca);
        let current = f.current(PeerRole::Voter);
        let leaf = f.issue(&f.key, 1, PeerRole::Voter, 600, &["node.test", "more.test"]);
        let renewed = accept(&current, leaf, None).expect("a renewal");
        assert_eq!(renewed.identity, current.identity);
        assert!(renewed.leaf.expires_at > current.leaf.expires_at);
    }

    /// Everything a renewal must not change is refused, each for its own
    /// reason, and the current leaf is what stays.
    #[test]
    fn anything_but_a_later_end_is_refused() {
        let ca = new_ca();
        let f = fixture(&ca);
        let current = f.current(PeerRole::Observer);
        let other = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let why = refused(accept(
            &current,
            f.issue(&other, 1, PeerRole::Observer, 600, &["node.test"]),
            None,
        ));
        assert!(why.contains("another key"), "{why}");
        let why = refused(accept(
            &current,
            f.issue(&f.key, 2, PeerRole::Observer, 600, &["node.test"]),
            None,
        ));
        assert!(why.contains("another incarnation"), "{why}");
        // An observer or collector does not acquire a voter's entitlement
        // by being handed a voter's certificate.
        let why = refused(accept(
            &current,
            f.issue(&f.key, 1, PeerRole::Voter, 600, &["node.test"]),
            None,
        ));
        assert!(why.contains("another role"), "{why}");
        let why = refused(accept(
            &current,
            f.issue(&f.key, 1, PeerRole::Observer, 600, &["elsewhere.test"]),
            None,
        ));
        assert!(why.contains("drops a name"), "{why}");
        let why = refused(accept(
            &current,
            f.issue(&f.key, 1, PeerRole::Observer, 30, &["node.test"]),
            None,
        ));
        assert!(why.contains("does not outlive"), "{why}");
        // Signed by nobody this node trusts.
        let stranger = new_ca();
        let why = refused(accept(
            &current,
            fixture(&stranger).issue(&f.key, 1, PeerRole::Observer, 600, &["node.test"]),
            None,
        ));
        assert!(why.contains("trust bundle"), "{why}");
    }

    #[test]
    fn a_voter_s_renewed_key_has_to_be_the_committed_one() {
        let ca = new_ca();
        let f = fixture(&ca);
        let current = f.current(PeerRole::Voter);
        let manifest = |key: &[u8]| {
            serde_json::from_value::<coord_membership::genesis::GenesisManifest>(
                serde_json::json!({
                    "cluster": hex(&CLUSTER),
                    "domain": hex(&[0x22; 16]),
                    "epoch": 1,
                    "voters": [{
                        "node": hex(&[1; 16]),
                        "incarnation": 1,
                        "public_key": b64url(key),
                    }],
                    "issuer_roots": [b64url(&[0xca; 8])],
                    "wif_rules": [{ "issuer": "test" }],
                    "admin": hex(&[0xa; 16]),
                    "protocol_version": 1,
                }),
            )
            .unwrap()
        };
        let leaf = f.issue(&f.key, 1, PeerRole::Voter, 600, &["node.test"]);
        let committed = Membership::from_genesis(&manifest(&current.spki)).unwrap();
        assert!(accept(&current, leaf.clone(), Some(&committed)).is_ok());
        // Genesis committed some other key: whatever this node holds, its
        // peers would not bind it, and neither does this.
        let elsewhere = Membership::from_genesis(&manifest(&[7; 32])).unwrap();
        let why = refused(accept(&current, leaf, Some(&elsewhere)));
        assert!(why.contains("not as a renewal"), "{why}");
    }

    #[test]
    fn an_installed_chain_replaces_the_file_whole_and_keeps_its_mode() {
        let dir = std::env::temp_dir().join(format!("coordd-enroll-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.pem");
        std::fs::write(&path, "old").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        }
        let ca = new_ca();
        let f = fixture(&ca);
        let leaf = f.issue(&f.key, 1, PeerRole::Voter, 600, &["node.test"]);
        install(&path, std::slice::from_ref(&leaf)).unwrap();
        let read: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(read, vec![leaf]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o640);
        }
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "a staged file was left behind"
        );
    }

    #[test]
    fn base64_round_trips() {
        for n in 0..9 {
            let bytes: Vec<u8> = (0..n)
                .map(|i: u8| i.wrapping_mul(37).wrapping_add(200))
                .collect();
            assert_eq!(b64url_decode(&b64url(&bytes)).unwrap(), bytes);
        }
    }
}
