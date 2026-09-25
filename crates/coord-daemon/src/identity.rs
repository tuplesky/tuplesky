//! Loading this node's own credentials (design Section 22.2).
//!
//! The configuration names three paths; this reads them, once, at
//! startup, and turns them into the identity the transport presents and
//! validates peers against. Everything it refuses, it refuses here --
//! before a listener exists -- rather than at the first handshake, when
//! the failure would look like a peer problem.
//!
//! What it refuses is chosen by what the alternative would be:
//!
//! * A trust bundle with no certificates in it is an empty root store,
//!   which trusts nothing. A process that started with one would accept
//!   no peer at all and report itself healthy while doing so.
//! * A chain with no certificates has nothing to present, and the
//!   handshake would fail for every peer with no indication of why.
//! * A private key other people can read is a key that has already left
//!   this process's control. Reading it and carrying on would make the
//!   configuration's promise -- that credentials are held under the
//!   process's own account -- untrue and unremarked.
//! * A certificate that does not chain to the trust bundle, or whose key
//!   this process does not hold ([`verify`]), is not an identity at all:
//!   the node's replica is read out of it, so a leaf nothing trusted
//!   issued could claim to be any voter.

use std::path::Path;
use std::sync::Arc;

use coord_transport::{Class, ClientIdentity, LocalIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId};
use rustls::RootCertStore;
use rustls::server::WebPkiClientVerifier;
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, UnixTime};

use crate::config::IdentityConfig;

/// Why this node's credentials could not be loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityError {
    /// A named file could not be read.
    Unreadable {
        /// Which of the three.
        what: &'static str,
        /// The path, as configured. It is a path, not material.
        path: String,
        /// The reason, as the operating system gave it.
        reason: String,
    },
    /// A file parsed but held nothing of the kind that was wanted.
    Empty {
        /// Which of the three.
        what: &'static str,
        /// The path, as configured.
        path: String,
    },
    /// A file did not parse as PEM of the expected kind.
    Malformed {
        /// Which of the three.
        what: &'static str,
        /// The path, as configured.
        path: String,
    },
    /// A root the bundle names is not one this build will trust.
    UntrustedRoot {
        /// The path, as configured.
        path: String,
    },
    /// One half of the collector credential is named and the other is
    /// not. A certificate without its key cannot be presented, and a
    /// key without its certificate names nobody; either way the process
    /// would silently fall back to submitting as the node, which is a
    /// different principal.
    HalfACollectorCredential,
    /// The private key is readable by someone other than this process's
    /// own account.
    KeyIsShared {
        /// The path, as configured.
        path: String,
        /// The mode as found, so an operator can see what to change.
        mode: u32,
    },
    /// The node certificate does not chain to the trust bundle, so
    /// nothing this domain trusts vouches for the identity it names.
    NotIssuedByTrustBundle {
        /// The certificate's path, as configured.
        path: String,
        /// Why the chain did not verify.
        reason: String,
    },
    /// The private key is not the key the node certificate certifies.
    KeyDoesNotMatchCertificate {
        /// The key's path, as configured.
        path: String,
    },
}

impl core::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IdentityError::Unreadable { what, path, reason } => {
                write!(f, "cannot read the {what} at {path}: {reason}")
            }
            IdentityError::Empty { what, path } => {
                write!(f, "the {what} at {path} contains none")
            }
            IdentityError::Malformed { what, path } => {
                write!(f, "the {what} at {path} is not PEM of that kind")
            }
            IdentityError::UntrustedRoot { path } => {
                write!(f, "a root in the bundle at {path} is not usable as one")
            }
            IdentityError::HalfACollectorCredential => f.write_str(
                "a collector credential needs both its certificate and its key, \
                 and this configuration names one of the two",
            ),
            IdentityError::KeyIsShared { path, mode } => write!(
                f,
                "the private key at {path} is readable beyond this account (mode {mode:o})"
            ),
            IdentityError::NotIssuedByTrustBundle { path, reason } => write!(
                f,
                "the certificate at {path} is not issued by the trust bundle: {reason}"
            ),
            IdentityError::KeyDoesNotMatchCertificate { path } => write!(
                f,
                "the private key at {path} is not the key the node certificate certifies"
            ),
        }
    }
}

impl core::error::Error for IdentityError {}

/// Read the credentials `config` names and build the local identity.
///
/// `capabilities` are the lane capabilities this endpoint grants and
/// `serves` is the plane it listens on; both come from the role, not
/// from a file. `replica` is this node's own identity where it has one,
/// which the endpoint uses only to settle a dial collision with a peer.
pub fn load(
    config: &IdentityConfig,
    cluster: ClusterId,
    domain: DomainId,
    capabilities: Vec<u16>,
    serves: Class,
    replica: Option<ReplicaId>,
) -> Result<LocalIdentity, IdentityError> {
    let roots = read_certificates("trust bundle", &config.trust_bundle)?;
    let chain = read_certificates("node certificate", &config.node_certificate)?;
    private_key_is_this_accounts("node key", &config.node_key)?;
    let key = PrivateKeyDer::from_pem_file(&config.node_key)
        .map_err(|e| pem_error("node key", &config.node_key, &e))?;

    let mut store = RootCertStore::empty();
    for root in roots {
        store.add(root).map_err(|_| IdentityError::UntrustedRoot {
            path: config.trust_bundle.clone(),
        })?;
    }

    Ok(LocalIdentity {
        cluster,
        domain,
        chain,
        key,
        roots: Arc::new(store),
        capabilities,
        api_client: collector_credential(config)?,
        serves: Some(serves),
        replica,
    })
}

/// Check that `identity` is one this domain vouches for: its leaf chains
/// to its own trust bundle, and its private key is the leaf's.
///
/// The node's replica and incarnation are read out of its certificate,
/// so the certificate is only an identity once something trusted issued
/// it. A self-signed leaf that merely claims a voter's node URI would
/// otherwise open, or initialize, that voter's store before any peer ever
/// saw the handshake that would have refused it; and a leaf whose key
/// this process does not hold would pass every local check and fail only
/// at the first handshake, where it reads as a peer problem. `config`
/// names the files, for the refusal.
pub fn verify(identity: &LocalIdentity, config: &IdentityConfig) -> Result<(), IdentityError> {
    verify_chain(&identity.chain, &identity.key, &identity.roots).map_err(|refusal| match refusal {
        ChainRefusal::Empty => IdentityError::Empty {
            what: "node certificate",
            path: config.node_certificate.clone(),
        },
        ChainRefusal::NotIssued(reason) => IdentityError::NotIssuedByTrustBundle {
            path: config.node_certificate.clone(),
            reason,
        },
        ChainRefusal::KeyMismatch => IdentityError::KeyDoesNotMatchCertificate {
            path: config.node_key.clone(),
        },
    })
}

/// Why a chain is not an identity this domain vouches for, before it is
/// attributed to any file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainRefusal {
    /// There is no leaf.
    Empty,
    /// The leaf does not chain to the roots now, with the reason.
    NotIssued(String),
    /// The key is not the leaf's.
    KeyMismatch,
}

impl core::fmt::Display for ChainRefusal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ChainRefusal::Empty => f.write_str("the chain holds no certificate"),
            ChainRefusal::NotIssued(reason) => {
                write!(f, "not issued by the trust bundle: {reason}")
            }
            ChainRefusal::KeyMismatch => f.write_str("the key is not the leaf's"),
        }
    }
}

/// Check that `chain` chains to `roots` now and that `key` is its
/// leaf's: what [`verify`] asks of the credential a node starts on, and
/// what a renewed leaf has to satisfy before a running node presents it
/// (task-d02). One check for both, so a renewal cannot put into service a
/// leaf a restart would refuse.
pub fn verify_chain(
    chain: &[CertificateDer<'static>],
    key: &PrivateKeyDer<'static>,
    roots: &Arc<RootCertStore>,
) -> Result<(), ChainRefusal> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let (leaf, intermediates) = chain.split_first().ok_or(ChainRefusal::Empty)?;
    // The same verifier the transport authenticates peers with, so a
    // certificate this accepts is one every peer holding the same bundle
    // accepts too.
    let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
        .build()
        .map_err(|e| ChainRefusal::NotIssued(e.to_string()))?;
    verifier
        .verify_client_cert(leaf, intermediates, UnixTime::now())
        .map_err(|e| ChainRefusal::NotIssued(e.to_string()))?;
    let certified = CertifiedKey::from_der(chain.to_vec(), key.clone_key(), &provider)
        .map_err(|_| ChainRefusal::KeyMismatch)?;
    // `from_der` lets through a key whose public half it cannot derive;
    // here that is a question that must be answered, so an unknown is a
    // refusal too.
    certified
        .keys_match()
        .map_err(|_| ChainRefusal::KeyMismatch)
}

/// The credential this process presents when it dials another voter as
/// this domain's collector.
///
/// Read here, at startup, with the same key-permission rule as the
/// node's own: a credential this process would present is a credential
/// this process must hold alone.
fn collector_credential(config: &IdentityConfig) -> Result<Option<ClientIdentity>, IdentityError> {
    let (certificate, key) = match (&config.collector_certificate, &config.collector_key) {
        (Some(c), Some(k)) => (c, k),
        (None, None) => return Ok(None),
        _ => return Err(IdentityError::HalfACollectorCredential),
    };
    let chain = read_certificates("collector certificate", certificate)?;
    private_key_is_this_accounts("collector key", key)?;
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| pem_error("collector key", key, &e))?;
    Ok(Some(ClientIdentity { chain, key }))
}

/// Every certificate in a PEM file, refusing a file that holds none.
fn read_certificates(
    what: &'static str,
    path: &str,
) -> Result<Vec<CertificateDer<'static>>, IdentityError> {
    let certificates: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| pem_error(what, path, &e))?
        .collect::<Result<_, _>>()
        .map_err(|e| pem_error(what, path, &e))?;
    if certificates.is_empty() {
        return Err(IdentityError::Empty {
            what,
            path: path.to_owned(),
        });
    }
    Ok(certificates)
}

/// Turn a PEM failure into "could not read" or "is not that" -- an
/// operator's next step differs entirely between the two.
fn pem_error(
    what: &'static str,
    path: &str,
    error: &rustls_pki_types::pem::Error,
) -> IdentityError {
    match error {
        rustls_pki_types::pem::Error::Io(e) => IdentityError::Unreadable {
            what,
            path: path.to_owned(),
            reason: e.to_string(),
        },
        rustls_pki_types::pem::Error::NoItemsFound => IdentityError::Empty {
            what,
            path: path.to_owned(),
        },
        _ => IdentityError::Malformed {
            what,
            path: path.to_owned(),
        },
    }
}

/// Refuse a private key other accounts can read.
///
/// This is a check about the file, not about its contents, so it happens
/// before the key is read: a key that has already been disclosed is not
/// made safer by this process parsing it successfully.
#[cfg(unix)]
fn private_key_is_this_accounts(what: &'static str, path: &str) -> Result<(), IdentityError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(Path::new(path)).map_err(|e| IdentityError::Unreadable {
        what,
        path: path.to_owned(),
        reason: e.to_string(),
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(IdentityError::KeyIsShared {
            path: path.to_owned(),
            mode,
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn private_key_is_this_accounts(what: &'static str, path: &str) -> Result<(), IdentityError> {
    // Elsewhere the file's reachability is all that can be checked here;
    // the platform's own access control is what holds the key.
    std::fs::metadata(Path::new(path))
        .map(|_| ())
        .map_err(|e| IdentityError::Unreadable {
            what,
            path: path.to_owned(),
            reason: e.to_string(),
        })
}
