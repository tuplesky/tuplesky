//! Who this node is, and which domain it belongs to (design Sections
//! 10.1, 20.4, 22.1).
//!
//! Neither answer comes from a setting. The domain, its cluster and its
//! committed voters come from the genesis manifest, which is the thing
//! every replica agrees on; this node's own replica and incarnation come
//! from the URI SAN of the certificate it presents, which is what the
//! node issuer bound them to.
//!
//! Reading the identity out of the certificate rather than out of the
//! configuration is the point. A node that could be told who it was
//! could be told it was somebody else: two processes configured with the
//! same replica identity would each open that replica's store, each vote
//! under it, and between them break the one thing a replica promises.
//! The certificate cannot be handed round that way, because the peers
//! that matter check it -- and so does this process, before it opens a
//! store under the identity: `main` refuses a certificate the trust
//! bundle did not issue, or whose key it does not hold, since reading an
//! identity out of a leaf anybody could sign would be the same as being
//! told it.

use coord_membership::genesis::GenesisManifest;
use coord_membership::membership::Membership;
use coord_node_issuer::parse_node_uri;
use coord_types::ids::{ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use x509_parser::prelude::FromDer;

/// Why this node could not establish who it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityError {
    /// The genesis manifest could not be read or parsed.
    Manifest {
        /// Where this node looked.
        path: String,
        /// Why.
        reason: String,
    },
    /// The manifest parsed but does not describe a usable configuration.
    Membership {
        /// Why, from the membership rules.
        reason: String,
    },
    /// The node certificate carries no node-identity URI, so nothing says
    /// which replica this process is.
    NoNodeIdentity {
        /// Where the certificate is.
        path: String,
    },
    /// The certificate names another cluster than the genesis does.
    ForeignCluster {
        /// Where the certificate is.
        path: String,
    },
    /// This node's role votes, but the committed configuration does not
    /// name it as a voter at this incarnation.
    NotACommittedVoter {
        /// The replica the certificate names.
        replica: ReplicaId,
        /// The incarnation it names.
        incarnation: ReplicaIncarnation,
    },
    /// The committed configuration names this node as a voter, but
    /// commits to a different key than the one it holds.
    NotTheCommittedKey {
        /// The replica the certificate names.
        replica: ReplicaId,
    },
}

impl core::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IdentityError::Manifest { path, reason } => {
                write!(f, "cannot read the genesis manifest at {path}: {reason}")
            }
            IdentityError::Membership { reason } => {
                write!(
                    f,
                    "the genesis manifest is not a usable configuration: {reason}"
                )
            }
            IdentityError::NoNodeIdentity { path } => write!(
                f,
                "the certificate at {path} carries no node identity: nothing says which replica this is"
            ),
            IdentityError::ForeignCluster { path } => write!(
                f,
                "the certificate at {path} belongs to another cluster than this genesis"
            ),
            IdentityError::NotACommittedVoter {
                replica,
                incarnation,
            } => write!(
                f,
                "this node votes, but the committed configuration does not name \
                 {} at incarnation {} as a voter",
                hex(&replica.0),
                incarnation.get()
            ),
            IdentityError::NotTheCommittedKey { replica } => write!(
                f,
                "the committed configuration names {} as a voter but commits to a \
                 different key than this node holds: no peer would accept it",
                hex(&replica.0)
            ),
        }
    }
}

impl core::error::Error for IdentityError {}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// This node, placed in its domain.
pub struct Placed {
    /// The manifest the configuration was read from, which the store
    /// pins at initialization and matches on every later start.
    pub manifest: GenesisManifest,
    /// The committed configuration this node starts from.
    pub membership: Membership,
    /// The replica this node is, as its own certificate says.
    pub replica: ReplicaId,
    /// The incarnation the certificate binds.
    pub incarnation: ReplicaIncarnation,
    /// The role the certificate was issued for.
    pub role: PeerRole,
}

/// Read the genesis manifest and this node's certificate, and say who
/// this node is.
///
/// `votes` is whether the configured role includes voting; a process
/// that votes must be a current committed voter, and one that does not
/// need only be of this cluster.
pub fn place(
    manifest_path: &str,
    certificate_path: &str,
    votes: bool,
) -> Result<Placed, IdentityError> {
    let text = std::fs::read(manifest_path).map_err(|e| IdentityError::Manifest {
        path: manifest_path.to_owned(),
        reason: e.to_string(),
    })?;
    let manifest: GenesisManifest =
        serde_json::from_slice(&text).map_err(|e| IdentityError::Manifest {
            path: manifest_path.to_owned(),
            reason: e.to_string(),
        })?;
    let membership =
        Membership::from_genesis(&manifest).map_err(|e| IdentityError::Membership {
            reason: format!("{e:?}"),
        })?;

    let (identity, leaf) = leaf_identity(certificate_path)?;
    // The cluster is checked here and not only at the handshake: a node
    // of another cluster would otherwise open this domain's store under
    // its own identity before any peer ever saw its certificate.
    if identity.cluster != membership.cluster() {
        return Err(IdentityError::ForeignCluster {
            path: certificate_path.to_owned(),
        });
    }
    if votes {
        if !membership.is_current_voter(&identity.node, identity.incarnation) {
            return Err(IdentityError::NotACommittedVoter {
                replica: identity.node,
                incarnation: identity.incarnation,
            });
        }
        // Genesis commits to the key, not merely to the name. A voter
        // holding some other key starts perfectly well -- nothing it
        // does alone checks this -- and is then refused by every peer it
        // meets, which looks like a network problem. It is this node's
        // own problem and it is knowable here.
        let presented = spki(&leaf);
        if !membership.is_current_voter_key(&identity.node, identity.incarnation, &presented) {
            return Err(IdentityError::NotTheCommittedKey {
                replica: identity.node,
            });
        }
    }
    Ok(Placed {
        manifest,
        membership,
        replica: identity.node,
        incarnation: identity.incarnation,
        role: identity.role,
    })
}

/// A certificate's SubjectPublicKeyInfo, which is what genesis commits
/// to for a voter and what a peer compares against.
fn spki(leaf: &CertificateDer<'_>) -> Vec<u8> {
    x509_parser::certificate::X509Certificate::from_der(leaf)
        .map(|(_, x509)| x509.public_key().raw.to_vec())
        .unwrap_or_default()
}

/// The node identity in the leaf certificate's URI SAN, and the leaf.
fn leaf_identity(
    path: &str,
) -> Result<(coord_node_issuer::NodeIdentity, CertificateDer<'static>), IdentityError> {
    let leaf = CertificateDer::pem_file_iter(path)
        .map_err(|e| IdentityError::Manifest {
            path: path.to_owned(),
            reason: format!("{e:?}"),
        })?
        .next()
        .and_then(Result::ok)
        .ok_or_else(|| IdentityError::NoNodeIdentity {
            path: path.to_owned(),
        })?;
    let (_, x509) = x509_parser::certificate::X509Certificate::from_der(&leaf).map_err(|_| {
        IdentityError::NoNodeIdentity {
            path: path.to_owned(),
        }
    })?;
    let san = x509
        .subject_alternative_name()
        .ok()
        .flatten()
        .ok_or_else(|| IdentityError::NoNodeIdentity {
            path: path.to_owned(),
        })?;
    for name in &san.value.general_names {
        if let x509_parser::extensions::GeneralName::URI(uri) = name
            && let Some(identity) = parse_node_uri(uri)
        {
            return Ok((identity, leaf.clone()));
        }
    }
    Err(IdentityError::NoNodeIdentity {
        path: path.to_owned(),
    })
}
