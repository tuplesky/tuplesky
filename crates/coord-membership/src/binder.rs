//! The production peer TLS identity binder (design Sections 10.1, 10.5,
//! 20.4). Called after ordinary certificate validation, it reads the
//! node-identity URI SAN from the peer's certificate and binds it to the
//! declared role, subject to committed membership.
//!
//! Peer access is never voting access. The binder accepts a well-formed
//! node certificate of the right cluster and domain for any peer role,
//! but a `Voter` declaration is bound only when the certificate's node
//! and incarnation are the *current* committed voter: a stale generation,
//! an unknown node, or a frontend, observer or learner certificate
//! claiming a voter slot is refused. What ultimately counts a vote is
//! consensus over the committed configuration; the binder only ensures a
//! frame's provenance is an exact, current identity, and a cloned
//! identity carries the same incarnation so it is deduplicated, never
//! counted twice.

use std::sync::{Arc, RwLock};

use coord_node_issuer::parse_node_uri;
use coord_transport::identity::role_class;
use coord_transport::{BindError, BoundIdentity, IdentityBinder};
use coord_types::ids::{ClusterId, DomainId};
use coord_types::wire_v1::{HelloV1, PeerRole};
use rustls_pki_types::CertificateDer;
use x509_parser::prelude::FromDer;

use crate::membership::Membership;

/// The peer binder: cluster, domain and the current committed membership
/// (swapped in on a committed handoff).
pub struct PeerBinder {
    cluster: ClusterId,
    domain: DomainId,
    membership: Arc<RwLock<Membership>>,
}

impl PeerBinder {
    /// A binder over `membership`.
    ///
    /// The cluster and domain come from the membership itself: passing
    /// them separately let the binder enforce an origin the committed
    /// membership did not agree with, and a later handoff silently kept
    /// the old pair.
    pub fn new(membership: Membership) -> Self {
        PeerBinder {
            cluster: membership.cluster(),
            domain: membership.domain(),
            membership: Arc::new(RwLock::new(membership)),
        }
    }

    /// A handle whose membership can be swapped (committed handoff).
    pub fn shared(&self) -> Arc<RwLock<Membership>> {
        self.membership.clone()
    }

    /// Install a new committed membership (a durably activated epoch).
    ///
    /// A membership of another cluster or domain is not a handoff of
    /// this one and is refused: the binder's origin is fixed by the
    /// genesis it started from.
    ///
    /// It governs the handshakes that follow and nothing already bound.
    /// A connection is bound once, at its handshake, and its deadline is
    /// fixed then from its own leaf's `notAfter` and the age cap; this
    /// holds no connection and ends none, so a peer bound under a key the
    /// new membership replaces keeps its session until that deadline,
    /// not until the rotation overlap (`RenewalPolicy::retire_at`) ends.
    /// Nothing in production calls this yet: re-arming or disconnecting
    /// such a peer belongs with the caller that installs memberships into
    /// a running node, the membership-activation task (task-m03).
    pub fn install(&self, membership: Membership) -> bool {
        if membership.cluster() != self.cluster || membership.domain() != self.domain {
            return false;
        }
        *self.membership.write().expect("membership lock") = membership;
        true
    }
}

/// The certificate's SubjectPublicKeyInfo DER: what genesis commits to
/// for a voter, and what the peer must present.
fn spki_of(cert: &CertificateDer<'_>) -> Option<Vec<u8>> {
    let (_, x509) = x509_parser::certificate::X509Certificate::from_der(cert).ok()?;
    Some(x509.public_key().raw.to_vec())
}

fn node_uri_of(cert: &CertificateDer<'_>) -> Option<coord_node_issuer::NodeIdentity> {
    let (_, x509) = x509_parser::certificate::X509Certificate::from_der(cert).ok()?;
    let san = x509.subject_alternative_name().ok()??;
    for name in &san.value.general_names {
        if let x509_parser::extensions::GeneralName::URI(uri) = name
            && let Some(identity) = parse_node_uri(uri)
        {
            return Some(identity);
        }
    }
    None
}

impl IdentityBinder for PeerBinder {
    fn bind(
        &self,
        certs: &[CertificateDer<'_>],
        hello: &HelloV1,
    ) -> Result<BoundIdentity, BindError> {
        let leaf = certs.first().ok_or(BindError::NoCertificate)?;
        let identity = node_uri_of(leaf).ok_or(BindError::UnknownCertificate)?;
        // The certificate's own bindings decide cluster, role and
        // incarnation; the Hello must not claim anything else.
        if identity.cluster != self.cluster || hello.cluster_id != self.cluster {
            return Err(BindError::ClusterMismatch);
        }
        if hello.domain_id != self.domain {
            return Err(BindError::DomainMismatch);
        }
        if hello.role != identity.role {
            return Err(BindError::RoleNotAuthorized);
        }
        let peer = role_class(identity.role) == coord_transport::Class::Peer;
        if peer && hello.incarnation != Some(identity.incarnation) {
            return Err(BindError::IncarnationMismatch);
        }
        // A voter certificate must name the current committed voter: the
        // exact node at the exact key generation. A stale generation or a
        // node that is not a committed voter is refused here.
        if identity.role == PeerRole::Voter {
            // The committed key, not merely the committed name: without
            // this, any certificate the issuer signs for that node at
            // that incarnation is accepted as the voter, so an issuer
            // that is compromised or merely tricked mints a peer of an
            // existing cluster.
            let presented = spki_of(leaf).ok_or(BindError::UnknownCertificate)?;
            let membership = self.membership.read().expect("membership lock");
            // One rule, named cases (task-58). A renewal binds: it is
            // the committed generation presenting the committed key,
            // and a node renews continuously. Everything else does not,
            // and the peer learns only that -- which distinction it was
            // is the node operator's business, reported by `coordd
            // inspect` on the node itself, not something to tell
            // whoever just failed to bind.
            if membership.classify_credential(&identity.node, identity.incarnation, &presented)
                != crate::membership::CredentialChange::Renewal
            {
                return Err(BindError::IncarnationMismatch);
            }
        }
        Ok(BoundIdentity {
            role: identity.role,
            replica: peer.then_some(identity.node),
            incarnation: peer.then_some(identity.incarnation),
            capabilities: hello.capabilities.as_slice().to_vec(),
        })
    }

    /// The leaf's `notAfter`, which is when a connection authenticated
    /// by it stops being authenticated (task-58).
    ///
    /// The transport closes at this instant. Certificate validation
    /// happens once, at the handshake; a connection that outlived its
    /// credential would be running on a check nobody repeated, which is
    /// precisely what renewal, rotation and revocation are meant to
    /// reach.
    fn expires_at(&self, certs: &[CertificateDer<'_>]) -> Option<u64> {
        let leaf = certs.first()?;
        let (_, x509) = x509_parser::certificate::X509Certificate::from_der(leaf).ok()?;
        u64::try_from(x509.validity().not_after.timestamp()).ok()
    }
}
