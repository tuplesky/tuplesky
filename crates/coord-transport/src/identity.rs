//! Role negotiation and identity binding.
//!
//! The adapter decides the connection class from the ALPN and admits a
//! declared role only in its class. Whether the TLS peer certificate is
//! entitled to that role, cluster, domain and incarnation is the
//! [`IdentityBinder`]'s decision: the runtime supplies the production
//! binder (WIF-issued node certificates, task-41/42) and tests supply an
//! isolated one. Peer access is never voting access: a bound voter
//! identity only lets frames carry a provenance; membership and ballots
//! decide what counts.

use coord_types::ids::{ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{HelloV1, PeerRole};
use rustls_pki_types::CertificateDer;

use crate::config::Class;

/// The connection class a role belongs to.
pub const fn role_class(role: PeerRole) -> Class {
    match role {
        PeerRole::Client | PeerRole::Frontend | PeerRole::KineCollector => Class::Api,
        PeerRole::Voter | PeerRole::Observer | PeerRole::Learner => Class::Peer,
    }
}

/// The identity a connection is bound to after negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundIdentity {
    /// Admitted role.
    pub role: PeerRole,
    /// Replica identity for peer roles.
    pub replica: Option<ReplicaId>,
    /// Incarnation for peer roles.
    pub incarnation: Option<ReplicaIncarnation>,
    /// Capabilities granted.
    pub capabilities: Vec<u16>,
}

/// Why a certificate was not bound to the declared identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindError {
    /// No certificate presented.
    NoCertificate,
    /// The certificate is not one this binder knows.
    UnknownCertificate,
    /// The certificate is not entitled to the declared role.
    RoleNotAuthorized,
    /// The declared cluster is not the certificate's.
    ClusterMismatch,
    /// The declared domain is not one the certificate may reach.
    DomainMismatch,
    /// The declared incarnation is not the certificate's.
    IncarnationMismatch,
}

/// Binds a TLS peer certificate to the identity a `Hello` declares.
pub trait IdentityBinder: Send + Sync {
    /// Decide whether `certs` (the peer's presented chain, leaf first)
    /// may act as `hello` declares. Called after ordinary certificate
    /// validation succeeded.
    fn bind(
        &self,
        certs: &[CertificateDer<'_>],
        hello: &HelloV1,
    ) -> Result<BoundIdentity, BindError>;
}
