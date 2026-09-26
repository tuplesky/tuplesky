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
    /// The extended key usages the leaf carries, by OID.
    usages: BTreeSet<String>,
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
            usages: read.usages,
        })
    }
}

/// What a leaf says, as far as renewal compares it.
struct ReadLeaf {
    identity: NodeIdentity,
    leaf: Leaf,
    spki: Vec<u8>,
    names: (BTreeSet<String>, BTreeSet<IpAddr>),
    /// Extended key usages by OID; empty where the leaf has no such
    /// extension, which X.509 reads as any usage.
    usages: BTreeSet<String>,
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
    let mut usages = BTreeSet::new();
    if let Ok(Some(eku)) = x509.extended_key_usage() {
        let known = [
            (eku.value.any, "2.5.29.37.0"),
            (eku.value.server_auth, "1.3.6.1.5.5.7.3.1"),
            (eku.value.client_auth, "1.3.6.1.5.5.7.3.2"),
            (eku.value.code_signing, "1.3.6.1.5.5.7.3.3"),
            (eku.value.email_protection, "1.3.6.1.5.5.7.3.4"),
            (eku.value.time_stamping, "1.3.6.1.5.5.7.3.8"),
            (eku.value.ocsp_signing, "1.3.6.1.5.5.7.3.9"),
        ];
        usages.extend(
            known
                .into_iter()
                .filter(|(present, _)| *present)
                .map(|(_, oid)| oid.to_owned()),
        );
        usages.extend(eku.value.other.iter().map(ToString::to_string));
    }
    Some(ReadLeaf {
        identity: identity?,
        leaf: Leaf {
            issued_at: u64::try_from(x509.validity().not_before.timestamp()).unwrap_or(0),
            expires_at: u64::try_from(x509.validity().not_after.timestamp()).unwrap_or(0),
        },
        spki: x509.public_key().raw.to_vec(),
        names: (dns, ips),
        usages,
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
    url: reqwest::Url,
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
        probe_certificate(Path::new(certificate))?;
        // Parsed here, as the client will parse it, so a URL it would
        // refuse stops the node at startup rather than at the first due
        // attempt, hours later, to be retried until the leaf runs out.
        let url = format!("{}/enroll", config.issuer.trim_end_matches('/'));
        let url = reqwest::Url::parse(&url)
            .ok()
            .filter(|u| {
                u.host_str().is_some_and(|h| !h.is_empty()) && u.port_or_known_default().is_some()
            })
            .ok_or_else(|| {
                format!(
                    "the issuer URL {} is not one this node can reach",
                    config.issuer
                )
            })?;
        Ok(Enroller {
            client,
            url,
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
            // The role this leaf already has. One workload can hold two
            // credentials for the same node -- the node's and the
            // collector's -- and presents one assertion for both, so the
            // issuer is told which of its rules for that workload
            // answers. It grants nothing a rule does not.
            "role": coord_node_issuer::role_str(current.identity.role),
        });
        let mut response = self
            .client
            .post(self.url.clone())
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
    // What the leaf may be used for, as well as where: a node is a TLS
    // server to its callers and peers and a client to the peers it dials,
    // so a renewal restricted to one of those (clientAuth only, say) would
    // pass the chain check -- which verifies it as a client certificate --
    // and then fail every handshake of the other kind. No usages at all
    // is X.509's "any", and a renewal that adds a restriction is refused.
    let narrowed = if current.usages.is_empty() {
        !read.usages.is_empty()
    } else {
        !read.usages.is_empty() && !current.usages.is_subset(&read.usages)
    };
    if narrowed {
        return Err(refused("it drops a key usage the current leaf is used for"));
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
        usages: read.usages,
    })
}

/// Check that `credential` is this node's collector: issued by the trust
/// bundle, held under its own key, naming this node at this incarnation,
/// in a role that may submit on a caller's behalf.
///
/// The node's own leaf gets the chain check at startup and is what the
/// node's identity is read from; the collector's leaf was only read. A
/// collector leaf for another node or incarnation was accepted and then
/// renewed faithfully, since a renewal is compared with the leaf it
/// replaces, not with the node. So the collector's leaf is tied to the
/// node here, once, before anything presents it (task-d02).
pub fn collector_for(
    credential: &Credential,
    cluster: coord_types::ids::ClusterId,
    node: coord_types::ids::ReplicaId,
    incarnation: coord_types::ids::ReplicaIncarnation,
) -> Result<(), String> {
    coord_daemon::verify_chain(&credential.chain, &credential.key, &credential.roots)
        .map_err(|e| e.to_string())?;
    let named = &credential.identity;
    if named.cluster != cluster || named.node != node {
        return Err("it names another node".into());
    }
    if named.incarnation != incarnation {
        return Err(format!(
            "it names incarnation {}, and this node is incarnation {}",
            named.incarnation.get(),
            incarnation.get()
        ));
    }
    match named.role {
        coord_types::wire_v1::PeerRole::Frontend
        | coord_types::wire_v1::PeerRole::KineCollector => Ok(()),
        other => Err(format!(
            "it names the role {}, which may not submit on a caller's behalf",
            coord_node_issuer::role_str(other)
        )),
    }
}

/// Whether the chain at `path` is one renewal can replace: a file of
/// certificates only, which this process can write beside and over.
///
/// Checked when the enroller is built, at startup and under `--check`,
/// rather than found at the first due point. A file that also holds the
/// node's key would be rewritten with the renewed chain alone -- the key
/// gone from disk, the process serving on from memory, and the next start
/// refused, which for a voter is recoverable only through a committed
/// replacement. A read-only mount would fail every attempt until the leaf
/// ran out.
fn probe_certificate(path: &Path) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "cannot read the node certificate at {}: {e}",
            path.display()
        )
    })?;
    for line in text.lines() {
        if let Some(label) = line
            .trim()
            .strip_prefix("-----BEGIN ")
            .and_then(|rest| rest.strip_suffix("-----"))
            && label != "CERTIFICATE"
        {
            return Err(format!(
                "the node certificate at {} also holds a {label} section, and renewal rewrites \
                 that file with certificates only; keep the key in a file of its own",
                path.display()
            ));
        }
    }
    let metadata = std::fs::metadata(path).map_err(|e| {
        format!(
            "cannot read the node certificate at {}: {e}",
            path.display()
        )
    })?;
    if metadata.permissions().readonly() {
        return Err(format!(
            "the node certificate at {} is read-only, and renewal replaces it",
            path.display()
        ));
    }
    let (directory, staged) = staging(path)
        .ok_or_else(|| format!("the certificate path {} names no file", path.display()))?;
    std::fs::File::create(&staged)
        .and_then(|_| std::fs::remove_file(&staged))
        .map_err(|e| {
            format!(
                "renewal cannot write beside the node certificate in {}: {e}",
                directory.display()
            )
        })
}

/// The directory `path` is in, and the file a renewed chain is staged in
/// before it is renamed over `path`.
fn staging(path: &Path) -> Option<(PathBuf, PathBuf)> {
    let name = path.file_name()?;
    let directory = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let staged = directory.join(format!(".{}.renewing", name.to_string_lossy()));
    Some((directory, staged))
}

/// Replace the chain at `path` with `chain`, atomically.
///
/// Written beside the file and renamed over it, with the file's own
/// permissions, so a crash leaves either the old chain or the new one and
/// never half of either -- a start reads this file, and a torn one would
/// be a node that cannot come back at all.
fn install(path: &Path, chain: &[CertificateDer<'static>]) -> Result<(), Failure> {
    use std::io::Write;

    let failed = |e: std::io::Error| Failure::Install(format!("{}: {}", path.display(), e.kind()));
    let (directory, staged) = staging(path).ok_or_else(|| {
        Failure::Install(format!(
            "the certificate path {} names no file",
            path.display()
        ))
    })?;
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
            self.issue_for(
                key,
                incarnation,
                role,
                secs,
                names,
                vec![
                    rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                    rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                ],
            )
        }

        /// The same, with these extended key usages.
        fn issue_for(
            &self,
            key: &rcgen::KeyPair,
            incarnation: u64,
            role: PeerRole,
            secs: i64,
            names: &[&str],
            usages: Vec<rcgen::ExtendedKeyUsagePurpose>,
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
            params.extended_key_usages = usages;
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

    /// A collector leaf is this node's only if it chains to the trust
    /// bundle under its own key and names this node, at this incarnation,
    /// in a submitting role; anything else is refused for what it is.
    #[test]
    fn a_collector_leaf_must_be_this_nodes() {
        use coord_types::ids::{ClusterId, ReplicaId, ReplicaIncarnation};
        let ca = new_ca();
        let f = fixture(&ca);
        let cluster = ClusterId(CLUSTER);
        let node = ReplicaId([1; 16]);
        let first = ReplicaIncarnation::new(1).unwrap();
        for role in [PeerRole::Frontend, PeerRole::KineCollector] {
            collector_for(&f.current(role), cluster, node, first).expect("this node's collector");
        }
        let why = collector_for(
            &f.current(PeerRole::Frontend),
            cluster,
            ReplicaId([2; 16]),
            first,
        )
        .unwrap_err();
        assert!(why.contains("another node"), "{why}");
        let why = collector_for(
            &f.current(PeerRole::Frontend),
            ClusterId([0x22; 16]),
            node,
            first,
        )
        .unwrap_err();
        assert!(why.contains("another node"), "{why}");
        let why = collector_for(
            &f.current(PeerRole::Frontend),
            cluster,
            node,
            ReplicaIncarnation::new(2).unwrap(),
        )
        .unwrap_err();
        assert!(why.contains("incarnation 1"), "{why}");
        let why = collector_for(&f.current(PeerRole::Voter), cluster, node, first).unwrap_err();
        assert!(why.contains("role voter"), "{why}");
        // Issued by another authority: the leaf names this node, and
        // nothing this domain trusts said so.
        let stranger = new_ca();
        let mut foreign = fixture(&stranger).current(PeerRole::Frontend);
        let mut roots = RootCertStore::empty();
        roots.add(ca.der.clone()).unwrap();
        foreign.roots = Arc::new(roots);
        let why = collector_for(&foreign, cluster, node, first).unwrap_err();
        assert!(why.contains("not issued by the trust bundle"), "{why}");
        // Held under a key that is not the leaf's.
        let mut unheld = f.current(PeerRole::Frontend);
        let other = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        unheld.key = Arc::new(
            PrivateKeyDer::from_pem_slice(pem("PRIVATE KEY", &other.serialize_der()).as_bytes())
                .unwrap(),
        );
        let why = collector_for(&unheld, cluster, node, first).unwrap_err();
        assert!(why.contains("not the leaf's"), "{why}");
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

    /// A leaf only good as a client would pass the chain check, which
    /// verifies it as one, and then fail every handshake this node serves.
    #[test]
    fn a_renewal_that_drops_a_key_usage_is_refused() {
        let ca = new_ca();
        let f = fixture(&ca);
        let current = f.current(PeerRole::Voter);
        let why = refused(accept(
            &current,
            f.issue_for(
                &f.key,
                1,
                PeerRole::Voter,
                600,
                &["node.test"],
                vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth],
            ),
            None,
        ));
        assert!(why.contains("key usage"), "{why}");
        // Adding one is not narrowing.
        let renewed = f.issue_for(
            &f.key,
            1,
            PeerRole::Voter,
            600,
            &["node.test"],
            vec![
                rcgen::ExtendedKeyUsagePurpose::ServerAuth,
                rcgen::ExtendedKeyUsagePurpose::ClientAuth,
                rcgen::ExtendedKeyUsagePurpose::CodeSigning,
            ],
        );
        accept(&current, renewed, None).expect("a renewal");
    }

    /// A certificate file renewal could not replace safely stops the node
    /// when the enroller is built, not at the first due point.
    #[test]
    fn a_certificate_file_renewal_cannot_replace_is_refused_up_front() {
        let dir = std::env::temp_dir().join(format!("coordd-probe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ca = new_ca();
        let f = fixture(&ca);
        let leaf = f.issue(&f.key, 1, PeerRole::Voter, 600, &["node.test"]);
        let certificate = pem("CERTIFICATE", &leaf);

        let alone = dir.join("alone.pem");
        std::fs::write(&alone, &certificate).unwrap();
        probe_certificate(&alone).expect("a file of certificates only");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "the probe left a file behind"
        );

        // The key beside the chain in one file would be written away.
        let combined = dir.join("combined.pem");
        std::fs::write(
            &combined,
            format!(
                "{certificate}{}",
                pem("PRIVATE KEY", &f.key.serialize_der())
            ),
        )
        .unwrap();
        let why = probe_certificate(&combined).expect_err("a key in the file");
        assert!(why.contains("PRIVATE KEY"), "{why}");

        let read_only = dir.join("read-only.pem");
        std::fs::write(&read_only, &certificate).unwrap();
        let mut permissions = std::fs::metadata(&read_only).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&read_only, permissions).unwrap();
        let why = probe_certificate(&read_only).expect_err("a read-only file");
        assert!(why.contains("read-only"), "{why}");
        let _ = std::fs::remove_dir_all(&dir);
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
