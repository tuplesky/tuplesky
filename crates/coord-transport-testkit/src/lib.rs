//! Isolated test certificates and identity binding for transport tests
//! (task-30). A private test CA issues node certificates; the binder maps
//! issued certificates to the identities they were issued for. Nothing
//! here is production issuance (task-41/42) and the crate is `test-only`:
//! the dependency policy keeps it out of every production edge, so a
//! test certificate can never be what a voter votes with. Keys are
//! Ed25519: fixed-length signatures keep handshake packet sizes stable,
//! which the packet-level simulator (task-32) relies on for trace
//! digests.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use coord_transport::{BindError, BoundIdentity, IdentityBinder, role_class};
use coord_transport::{Class, LocalIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{HelloV1, PeerRole};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Crate role marker used by the dependency-policy check.
pub const CRATE_ROLE: &str = "test-only";

/// A private certificate authority for one test.
pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    der: CertificateDer<'static>,
}

/// An identity issued by a [`TestCa`].
pub struct TestIdentity {
    /// DNS name (the server name peers connect with).
    pub name: String,
    /// Replica identity (peer roles).
    pub replica: ReplicaId,
    /// Incarnation (peer roles).
    pub incarnation: ReplicaIncarnation,
    /// Role the certificate is entitled to.
    pub role: PeerRole,
    /// Certificate chain (leaf only).
    pub chain: Vec<CertificateDer<'static>>,
    /// Private key.
    pub key: PrivateKeyDer<'static>,
}

impl TestIdentity {
    /// The local identity for a transport endpoint using this
    /// certificate under `ca`.
    pub fn local(
        &self,
        ca: &TestCa,
        cluster: ClusterId,
        domain: DomainId,
        capabilities: Vec<u16>,
    ) -> LocalIdentity {
        LocalIdentity {
            cluster,
            domain,
            chain: self.chain.clone(),
            key: self.key.clone_key(),
            roots: ca.roots(),
            capabilities,
        }
    }

    /// The identity a peer expects when connecting to this node.
    pub fn expected(&self) -> BoundIdentity {
        let peer = role_class(self.role) == Class::Peer;
        BoundIdentity {
            role: self.role,
            replica: peer.then_some(self.replica),
            incarnation: peer.then_some(self.incarnation),
            capabilities: Vec::new(),
        }
    }
}

impl Default for TestCa {
    fn default() -> Self {
        Self::new()
    }
}

impl TestCa {
    /// A fresh CA with a fresh key.
    pub fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, "tuplesky test ca");
        let key = KeyPair::generate_for(&rcgen::PKCS_ED25519).expect("ca key");
        let cert = params.self_signed(&key).expect("ca cert");
        let der = cert.der().clone();
        TestCa {
            issuer: Issuer::new(params, key),
            der,
        }
    }

    /// Trust anchors containing only this CA.
    pub fn roots(&self) -> Arc<rustls::RootCertStore> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.der.clone()).expect("ca root");
        Arc::new(roots)
    }

    /// Issue a certificate for `name` entitled to `role`.
    pub fn issue(
        &self,
        name: &str,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        role: PeerRole,
    ) -> TestIdentity {
        let mut params = CertificateParams::new(vec![name.to_string()]).expect("leaf params");
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate_for(&rcgen::PKCS_ED25519).expect("leaf key");
        let cert = params.signed_by(&key, &self.issuer).expect("leaf cert");
        TestIdentity {
            name: name.to_string(),
            replica,
            incarnation,
            role,
            chain: vec![cert.der().clone()],
            key: PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        }
    }
}

/// Binds issued certificates to the identities they were issued for.
pub struct TestBinder {
    cluster: ClusterId,
    domain: DomainId,
    known: HashMap<Vec<u8>, (ReplicaId, ReplicaIncarnation, PeerRole)>,
}

impl TestBinder {
    /// A binder for one cluster and domain.
    pub fn new(cluster: ClusterId, domain: DomainId) -> Self {
        TestBinder {
            cluster,
            domain,
            known: HashMap::new(),
        }
    }

    /// Register an issued identity.
    pub fn register(&mut self, identity: &TestIdentity) {
        self.known.insert(
            identity.chain[0].as_ref().to_vec(),
            (identity.replica, identity.incarnation, identity.role),
        );
    }
}

impl IdentityBinder for TestBinder {
    fn bind(
        &self,
        certs: &[CertificateDer<'_>],
        hello: &HelloV1,
    ) -> Result<BoundIdentity, BindError> {
        let leaf = certs.first().ok_or(BindError::NoCertificate)?;
        let (replica, incarnation, role) = self
            .known
            .get(leaf.as_ref())
            .ok_or(BindError::UnknownCertificate)?;
        if hello.role != *role {
            return Err(BindError::RoleNotAuthorized);
        }
        if hello.cluster_id != self.cluster {
            return Err(BindError::ClusterMismatch);
        }
        if hello.domain_id != self.domain {
            return Err(BindError::DomainMismatch);
        }
        let peer = role_class(*role) == Class::Peer;
        if peer && hello.incarnation != Some(*incarnation) {
            return Err(BindError::IncarnationMismatch);
        }
        Ok(BoundIdentity {
            role: *role,
            replica: peer.then_some(*replica),
            incarnation: peer.then_some(*incarnation),
            capabilities: hello.capabilities.as_slice().to_vec(),
        })
    }
}
