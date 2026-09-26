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
        self.local_trusting(ca.roots(), cluster, domain, capabilities)
    }

    /// The same, against trust anchors chosen by the caller.
    ///
    /// A staged CA rotation is exactly this: for a while an endpoint
    /// trusts both the outgoing and the incoming root, so leaves signed
    /// by either are accepted and nothing has to be restarted in
    /// lockstep; afterwards the old root is dropped and leaves under it
    /// stop being accepted (task-58).
    pub fn local_trusting(
        &self,
        roots: Arc<rustls::RootCertStore>,
        cluster: ClusterId,
        domain: DomainId,
        capabilities: Vec<u16>,
    ) -> LocalIdentity {
        LocalIdentity {
            cluster,
            domain,
            chain: self.chain.clone(),
            key: self.key.clone_key(),
            roots,
            capabilities,
            // One certificate, one principal: a test endpoint dials as
            // whatever it serves as.
            api_client: None,
            // And one endpoint is both planes here, so it offers both
            // ALPNs; a deployment's two listeners each offer their own.
            serves: None,
            // The identity this fixture's certificate names, so a test
            // mesh settles a dial collision the way a deployment does.
            replica: Some(self.replica),
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

    /// The CA certificate in DER, for tests that must hand the trust
    /// anchor to something outside this process.
    pub const fn certificate_der(&self) -> &CertificateDer<'static> {
        &self.der
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

/// Trust anchors containing every CA in `cas`.
///
/// What an endpoint holds partway through a staged CA rotation: both
/// the root being retired and the one replacing it, so leaves under
/// either are accepted while the fleet reissues.
pub fn roots_of(cas: &[&TestCa]) -> Arc<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for ca in cas {
        roots.add(ca.certificate_der().clone()).expect("ca root");
    }
    Arc::new(roots)
}

/// Binds issued certificates to the identities they were issued for.
pub struct TestBinder {
    cluster: ClusterId,
    domain: DomainId,
    known: HashMap<Vec<u8>, (ReplicaId, ReplicaIncarnation, PeerRole)>,
    /// When the credentials this binder admits stop being valid, in
    /// unix seconds (task-58). `None` leaves the connection bounded by
    /// the age cap alone, which is what every test that is not about
    /// credential expiry wants.
    expires_at: Option<u64>,
    /// Ends stated for one leaf, overriding `expires_at` for it.
    ends: HashMap<Vec<u8>, Option<u64>>,
}

impl TestBinder {
    /// A binder for one cluster and domain.
    pub fn new(cluster: ClusterId, domain: DomainId) -> Self {
        TestBinder {
            cluster,
            domain,
            known: HashMap::new(),
            expires_at: None,
            ends: HashMap::new(),
        }
    }

    /// Make every credential this binder admits end at `unix_seconds`.
    ///
    /// A real binder reads the leaf's `notAfter`; a test that had to
    /// mint a certificate expiring seconds from now would be a test
    /// about rcgen's clock. This says the same thing to the transport.
    pub const fn expiring_at(mut self, unix_seconds: u64) -> Self {
        self.expires_at = Some(unix_seconds);
        self
    }

    /// Register an issued identity whose credential ends at `until`
    /// (unix seconds; `None` for no stated end), whatever
    /// [`TestBinder::expiring_at`] says for the others.
    ///
    /// The transport asks the binder about its own leaf as well as its
    /// peers' (task-d02), so a test that is about one end's leaf needs
    /// the two answers to differ.
    pub fn register_until(&mut self, identity: &TestIdentity, until: Option<u64>) {
        self.register(identity);
        self.ends.insert(identity.chain[0].as_ref().to_vec(), until);
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

    fn expires_at(&self, certs: &[CertificateDer<'_>]) -> Option<u64> {
        certs
            .first()
            .and_then(|leaf| self.ends.get(leaf.as_ref()).copied())
            .unwrap_or(self.expires_at)
    }
}
