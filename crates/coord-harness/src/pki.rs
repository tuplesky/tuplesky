//! The fixture certificate authority a provisioned domain is issued
//! under (design Sections 3.1, 8.2).
//!
//! One root, everything else issued below it, exactly as a deployment
//! has it: a node certificate carries the node-identity URI the issuer
//! binds, a collector certificate names the same node in the role that
//! may submit on a caller's behalf, and the Kubernetes storage edge gets
//! its own server and client identities under a *separate* authority --
//! because the API server's client CA is not the domain's peer CA and a
//! harness that shared one would never notice the difference.

use std::path::Path;

use coord_types::ids::{ClusterId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;

/// A certificate and the key that owns it.
pub struct Issued {
    /// The leaf certificate.
    pub certificate: rcgen::Certificate,
    /// Its key pair.
    pub key: rcgen::KeyPair,
}

impl Issued {
    /// The leaf's `SubjectPublicKeyInfo`, which is what a genesis
    /// manifest commits to for a voter -- the name is not the identity.
    pub fn spki(&self) -> Vec<u8> {
        use x509_parser::prelude::FromDer;
        let (_, parsed) =
            x509_parser::certificate::X509Certificate::from_der(self.certificate.der())
                .expect("a certificate this process just issued parses");
        parsed.public_key().raw.to_vec()
    }

    /// Write the certificate and key as PEM, with the key readable only
    /// by its owner.
    pub fn write(&self, certificate: &Path, key: &Path) -> std::io::Result<()> {
        std::fs::write(certificate, pem("CERTIFICATE", self.certificate.der()))?;
        std::fs::write(key, pem("PRIVATE KEY", &self.key.serialize_der()))?;
        restrict(key)
    }
}

/// PEM, encoded here because the pinned `rcgen` is built without its
/// own PEM support: a certificate fixture is not a reason to widen a
/// dependency's feature set.
pub fn pem(label: &str, der: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut body = String::new();
    for chunk in der.chunks(3) {
        let bytes = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let packed = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                body.push(A[((packed >> (18 - 6 * i)) & 0x3f) as usize] as char);
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

/// Make a private key file readable only by the user running the
/// harness. A key a test wrote world-readable is still a key.
pub fn restrict(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// A certificate authority the harness controls.
pub struct Ca {
    key: rcgen::KeyPair,
    certificate: rcgen::Certificate,
}

impl Ca {
    /// The signing key, PKCS#8 DER. A run directory keeps it so that a
    /// benchmark can issue the caller credentials it needs without this
    /// process being alive to do it; it is a throwaway fixture
    /// authority, written with the same permissions as any other key
    /// here, and no deployment ever sees it.
    pub fn signing_key(&self) -> Vec<u8> {
        self.key.serialize_der()
    }

    /// Reopen an authority from its stored key and certificate.
    ///
    /// The certificate is parsed only to keep the trust anchor
    /// available; what signs is the key, and the issuer parameters are
    /// rebuilt exactly as [`Ca::new`] made them, so the subject a leaf
    /// names is the one the stored root has.
    pub fn reopen(signing_key: &[u8]) -> Option<Self> {
        let key = rcgen::KeyPair::try_from(signing_key).ok()?;
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).ok()?;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key).ok()?;
        Some(Ca { key, certificate })
    }

    /// A fresh self-signed authority.
    pub fn new() -> Self {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("ca key");
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key).expect("ca certificate");
        Ca { key, certificate }
    }

    /// The trust anchor, PEM-encoded.
    pub fn root_pem(&self) -> String {
        pem("CERTIFICATE", self.certificate.der())
    }

    /// Issue a leaf for `names` (the first is the common name), with the
    /// additional SANs the caller supplies.
    pub fn issue_leaf(&self, names: &[String], extra: Vec<rcgen::SanType>) -> Issued {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
        let mut params = rcgen::CertificateParams::new(names.to_vec()).expect("leaf params");
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        if let Some(first) = names.first() {
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, first.clone());
        }
        params.subject_alt_names.extend(extra);
        // The issuer borrows its own parameters, so it is built here
        // rather than handed back from a helper.
        let mut authority = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        authority.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let issuer = rcgen::Issuer::from_params(&authority, &self.key);
        let certificate = params.signed_by(&key, &issuer).expect("leaf certificate");
        Issued { certificate, key }
    }

    /// Issue a node credential: the identity URI is what says which
    /// replica and which role a process is, so a certificate without one
    /// is not an identity at all.
    pub fn issue_node(
        &self,
        name: &str,
        cluster: ClusterId,
        node: ReplicaId,
        incarnation: ReplicaIncarnation,
        role: PeerRole,
    ) -> Issued {
        // Loopback as well: a catalog lists addresses a process can
        // dial, and a harness has no DNS. It adds a way to reach the node
        // and no authority whatsoever -- the URI is what carries the
        // identity.
        self.issue_node_at(name, cluster, node, incarnation, role, loopback())
    }

    /// The same credential, reachable at `reach` instead of loopback.
    ///
    /// A peer dials the host part of the catalog address it was given
    /// and verifies the certificate against it, so a node listed at
    /// `10.0.0.2` or `n2.example` has to carry that address or that name
    /// or every handshake to it fails -- which reads as a network
    /// problem and is not. The name is still only a way to reach the
    /// node: the binder decides which voter answered from the URI, so a
    /// certificate valid for the right host and the wrong replica is
    /// refused exactly as before.
    pub fn issue_node_at(
        &self,
        name: &str,
        cluster: ClusterId,
        node: ReplicaId,
        incarnation: ReplicaIncarnation,
        role: PeerRole,
        reach: rcgen::SanType,
    ) -> Issued {
        let identity = coord_node_issuer::NodeIdentity {
            cluster,
            node,
            incarnation,
            role,
        };
        self.issue_leaf(
            &[name.to_owned()],
            vec![
                reach,
                rcgen::SanType::URI(
                    coord_node_issuer::node_uri(&identity)
                        .try_into()
                        .expect("a node URI is a URI"),
                ),
            ],
        )
    }

    /// Issue a server identity for a loopback listener.
    pub fn issue_server(&self, name: &str) -> Issued {
        self.issue_server_at(name, loopback())
    }

    /// Issue a server identity for a listener reached at `reach`.
    pub fn issue_server_at(&self, name: &str, reach: rcgen::SanType) -> Issued {
        self.issue_leaf(&[name.to_owned()], vec![reach])
    }
}

/// The loopback address, as a subject alternative name.
fn loopback() -> rcgen::SanType {
    rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
}

/// The subject alternative name a client checks when it dials `host`:
/// an IP address for an IP literal, a DNS name otherwise. `None` for a
/// string that is neither, so a typo in a host list is refused when the
/// domain is provisioned rather than at the first handshake.
pub fn reach(host: &str) -> Option<rcgen::SanType> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Some(rcgen::SanType::IpAddress(ip));
    }
    let valid = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        });
    if !valid {
        return None;
    }
    Some(rcgen::SanType::DnsName(host.try_into().ok()?))
}

/// Whether a DNS name only ever means the machine it is resolved on:
/// `localhost` and the names under it (RFC 6761, section 6.3).
pub fn loopback_name(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    host == "localhost" || host.ends_with(".localhost")
}

impl Default for Ca {
    fn default() -> Self {
        Ca::new()
    }
}
