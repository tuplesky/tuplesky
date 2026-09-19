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

use std::path::Path;
use std::sync::Arc;

use coord_transport::LocalIdentity;
use coord_types::ids::{ClusterId, DomainId};
use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

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
    /// The private key is readable by someone other than this process's
    /// own account.
    KeyIsShared {
        /// The path, as configured.
        path: String,
        /// The mode as found, so an operator can see what to change.
        mode: u32,
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
            IdentityError::KeyIsShared { path, mode } => write!(
                f,
                "the private key at {path} is readable beyond this account (mode {mode:o})"
            ),
        }
    }
}

impl core::error::Error for IdentityError {}

/// Read the credentials `config` names and build the local identity.
///
/// `capabilities` are the lane capabilities this endpoint grants; they
/// come from the role, not from a file.
pub fn load(
    config: &IdentityConfig,
    cluster: ClusterId,
    domain: DomainId,
    capabilities: Vec<u16>,
) -> Result<LocalIdentity, IdentityError> {
    let roots = read_certificates("trust bundle", &config.trust_bundle)?;
    let chain = read_certificates("node certificate", &config.node_certificate)?;
    private_key_is_this_accounts(&config.node_key)?;
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
    })
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
fn private_key_is_this_accounts(path: &str) -> Result<(), IdentityError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(Path::new(path)).map_err(|e| IdentityError::Unreadable {
        what: "node key",
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
fn private_key_is_this_accounts(path: &str) -> Result<(), IdentityError> {
    // Elsewhere the file's reachability is all that can be checked here;
    // the platform's own access control is what holds the key.
    std::fs::metadata(Path::new(path))
        .map(|_| ())
        .map_err(|e| IdentityError::Unreadable {
            what: "node key",
            path: path.to_owned(),
            reason: e.to_string(),
        })
}
