//! task-42 acceptance: a signed, pinned genesis initializes durable
//! membership; the peer binder binds node certificates issued by the WIF
//! issuer and refuses wrong origin, stale generation, and frontend,
//! observer or learner certificates claiming a voter slot; a cloned
//! identity carries the same incarnation and is never counted twice;
//! missing or rolled-back durable state quarantines rather than
//! reinitializing.

use std::collections::BTreeMap;

use coord_authn::ClockHealth;
use coord_membership::genesis::b64url;
use coord_membership::init::{GenesisStore, StoreFailure};
use coord_membership::{
    GenesisError, GenesisManifest, InitError, Membership, PeerBinder, SignedGenesis, VoterSeed,
    initialize, sign_genesis, verify_genesis,
};
use coord_node_issuer::{Ca, NodeIssuer, NodeRequest, RolePolicy};
use coord_transport::{BindError, IdentityBinder};
use coord_types::identity::Digest32;
use coord_types::ids::{ClusterId, DomainId, PrincipalId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::{BoundedVec, HelloV1, PeerRole};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey};
use rcgen::string::Ia5String;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};
use rustls_pki_types::CertificateDer;
use time::OffsetDateTime;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const NOW: u64 = 1_700_000_000;
const AUD: &str = "node-enrollment";
const K8S_ISS: &str = "https://kubernetes.default.svc";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn ecdsa() -> KeyPair {
    KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap()
}

// ---- an admin signing key for genesis (ES256 JWK) --------------------------

struct AdminKey {
    enc: EncodingKey,
    x: String,
    y: String,
}

fn admin_key() -> AdminKey {
    let key = ecdsa();
    let point = key.public_key_raw();
    AdminKey {
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        x: coord_sts::keys::b64url(&point[1..33]),
        y: coord_sts::keys::b64url(&point[33..65]),
    }
}

fn pinned(admin: &AdminKey) -> DecodingKey {
    DecodingKey::from_ec_components(&admin.x, &admin.y).unwrap()
}

// ---- the WIF issuer that mints node certificates ---------------------------

struct Issuer {
    issuer: NodeIssuer,
    idp_key: KeyPair,
    idp_kid: String,
    ca_der: Vec<u8>,
}

fn build_issuer() -> Issuer {
    let idp = ecdsa();
    let point = idp.public_key_raw();
    let jwks = serde_json::to_vec(&serde_json::json!({"keys": [{
        "kty": "EC", "crv": "P-256", "kid": "kk", "alg": "ES256", "use": "sig",
        "x": coord_sts::keys::b64url(&point[1..33]), "y": coord_sts::keys::b64url(&point[33..65]),
    }]}))
    .unwrap();
    let config = coord_authn::IssuerConfig {
        name: "k8s".into(),
        issuer: K8S_ISS.into(),
        jwks_url: "https://k8s/keys".into(),
        algorithms: vec![Algorithm::ES256],
        audiences: vec![AUD.into()],
        max_age_secs: Some(3600),
        allow_insecure_loopback: false,
    };
    let mut registry =
        coord_authn::Registry::new(vec![config], coord_authn::JwksLimits::default()).unwrap();
    registry.install_keys("k8s", &jwks, NOW).unwrap();
    let mut kinds = BTreeMap::new();
    kinds.insert(
        "k8s".to_string(),
        coord_authn::WorkloadKind::Kubernetes(coord_authn::KubernetesMode::Offline),
    );
    let verifier = coord_authn::WifVerifier::new(registry, kinds);
    // CA.
    let ca_key = ecdsa();
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "node CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params.not_before = OffsetDateTime::from_unix_timestamp(NOW as i64 - 10).unwrap();
    ca_params.not_after = OffsetDateTime::from_unix_timestamp(NOW as i64 + 86_400).unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_der = ca_cert.der().to_vec();
    let ca = Ca::load(&ca_der, &ca_key.serialize_der(), NOW).unwrap();
    let policy = |node: u8, role: PeerRole, ns: &str, sa: &str| RolePolicy {
        issuer: "k8s".into(),
        required: [("namespace", ns), ("serviceaccount", sa)]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        cluster: CLUSTER,
        nodes: vec![ReplicaId([node; 16])],
        role,
        min_incarnation: 1,
        max_lifetime_secs: 3600,
        dns_names: vec![],
        ip_addresses: vec![],
    };
    let rules = vec![
        policy(1, PeerRole::Voter, "voters", "voter-1"),
        policy(2, PeerRole::Voter, "voters", "voter-2"),
        policy(3, PeerRole::Voter, "voters", "voter-3"),
        policy(9, PeerRole::Observer, "observers", "observer-0"),
    ];
    Issuer {
        issuer: NodeIssuer::new(verifier, ca, rules),
        idp_key: idp,
        idp_kid: "kk".into(),
        ca_der,
    }
}

impl Issuer {
    fn assertion(&self, ns: &str, sa: &str) -> String {
        let mut h = jsonwebtoken::Header::new(Algorithm::ES256);
        h.kid = Some(self.idp_kid.clone());
        let claims = serde_json::json!({
            "iss": K8S_ISS, "sub": format!("system:serviceaccount:{ns}:{sa}"), "aud": AUD,
            "exp": NOW + 300, "iat": NOW,
            "kubernetes.io": {"namespace": ns, "serviceaccount": {"name": sa}},
        });
        jsonwebtoken::encode(
            &h,
            &claims,
            &jsonwebtoken::EncodingKey::from_ec_der(&self.idp_key.serialize_der()),
        )
        .unwrap()
    }

    /// Issue a certificate for a node; returns the leaf DER.
    fn issue(&mut self, node: u8, incarnation: u64, role: PeerRole, ns: &str, sa: &str) -> Vec<u8> {
        let key = ecdsa();
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, "csr");
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let csr = params.serialize_request(&key).unwrap();
        let _ = role;
        let request = NodeRequest {
            assertion: self.assertion(ns, sa),
            csr_der: csr.der().to_vec(),
            node: [node; 16],
            incarnation,
            lifetime_secs: 300,
            role: None,
        };
        self.issuer
            .enroll(&request, &ClockHealth::healthy(NOW, 5))
            .unwrap()
            .certificate
    }

    /// A certificate with no tuplesky node URI SAN (a stranger).
    fn stranger(&self) -> Vec<u8> {
        let key = ecdsa();
        let mut params = CertificateParams::new(vec!["stranger.example".to_string()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let _ = SanType::DnsName(Ia5String::try_from("stranger.example").unwrap());
        params.self_signed(&key).unwrap().der().to_vec()
    }
}

/// The SubjectPublicKeyInfo a certificate carries, as genesis commits it.
fn spki(cert_der: &[u8]) -> String {
    let (_, x509) = x509_parser::parse_x509_certificate(cert_der).unwrap();
    b64url(x509.public_key().raw)
}

/// A manifest committing the three founding voters by key, together with
/// the certificates those keys belong to.
fn manifest_with_voters(issuer: &mut Issuer, admin: &AdminKey) -> (GenesisManifest, Vec<Vec<u8>>) {
    let _ = admin;
    let certs: Vec<Vec<u8>> = (1u8..=3)
        .map(|n| issuer.issue(n, 1, PeerRole::Voter, "voters", &format!("voter-{n}")))
        .collect();
    let manifest = GenesisManifest {
        cluster: hex(&CLUSTER.0),
        domain: hex(&DOMAIN.0),
        epoch: 1,
        voters: (1u8..=3)
            .map(|n| VoterSeed {
                node: hex(&[n; 16]),
                incarnation: 1,
                public_key: spki(&certs[(n - 1) as usize]),
            })
            .collect(),
        issuer_roots: vec![b64url(&issuer.ca_der)],
        wif_rules: vec![serde_json::json!({
            "issuer": "k8s",
            "namespace": "voters",
            "serviceaccount": "voter-1",
            "scope_ceiling": 7,
        })],
        admin: hex(&PrincipalId([0xa; 16]).0),
        protocol_version: 1,
    };
    (manifest, certs)
}

fn hello(role: PeerRole, incarnation: Option<u64>) -> HelloV1 {
    HelloV1 {
        role,
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        incarnation: incarnation.map(|i| ReplicaIncarnation::new(i).unwrap()),
        capabilities: BoundedVec::new(vec![1]).unwrap(),
    }
}

fn der(bytes: &[u8]) -> CertificateDer<'static> {
    CertificateDer::from(bytes.to_vec())
}

#[derive(Default)]
struct Store {
    pinned: Option<Digest32>,
    journal: bool,
    fail: bool,
}

impl GenesisStore for Store {
    fn pinned_digest(&self) -> Result<Option<Digest32>, StoreFailure> {
        if self.fail {
            return Err(StoreFailure);
        }
        Ok(self.pinned)
    }
    fn pin(&mut self, digest: Digest32) -> Result<(), StoreFailure> {
        self.pinned = Some(digest);
        Ok(())
    }
    fn journal_intact(&self) -> Result<bool, StoreFailure> {
        Ok(self.journal)
    }
    fn establish_journal(&mut self) -> Result<(), StoreFailure> {
        self.journal = true;
        Ok(())
    }
}

#[test]
fn a_signed_genesis_initializes_pinned_membership() {
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, voter_certs) = manifest_with_voters(&mut issuer, &admin);
    let signed = sign_genesis(&manifest, &admin.enc).unwrap();
    // Verified against the pinned key; a different key or a tampered
    // token is refused.
    let verified = verify_genesis(&signed, &pinned(&admin), 1).unwrap();
    assert_eq!(verified, manifest);
    let other = admin_key();
    assert_eq!(
        verify_genesis(&signed, &pinned(&other), 1),
        Err(GenesisError::Signature)
    );
    let mut tampered = signed.0.clone();
    tampered.pop();
    tampered.push(if signed.0.ends_with('a') { 'b' } else { 'a' });
    assert!(verify_genesis(&SignedGenesis(tampered), &pinned(&admin), 1).is_err());
    // An unsupported protocol version.
    assert!(matches!(
        verify_genesis(&signed, &pinned(&admin), 2),
        Err(GenesisError::UnsupportedProtocol { version: 1 })
    ));
    // First boot pins; a returning boot with the same manifest re-opens;
    // a different manifest is a digest mismatch (quarantine); a lost
    // journal quarantines.
    let mut store = Store::default();
    let init = initialize(&verified, &mut store).unwrap();
    assert!(init.first_boot);
    assert_eq!(init.membership.epoch().get(), 1);
    let again = initialize(&verified, &mut store).unwrap();
    assert!(!again.first_boot);
    let mut other_manifest = manifest.clone();
    other_manifest.voters.push(VoterSeed {
        node: hex(&[4; 16]),
        incarnation: 1,
        public_key: spki(&voter_certs[0]),
    });
    let other_signed = sign_genesis(&other_manifest, &admin.enc).unwrap();
    let other_verified = verify_genesis(&other_signed, &pinned(&admin), 1).unwrap();
    assert!(matches!(
        initialize(&other_verified, &mut store),
        Err(InitError::DigestMismatch { .. })
    ));
    store.journal = false;
    assert_eq!(
        initialize(&verified, &mut store),
        Err(InitError::JournalLost)
    );
    let _ = &mut issuer;
}

#[test]
fn the_binder_admits_current_voters_and_refuses_everything_else() {
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, voter_certs) = manifest_with_voters(&mut issuer, &admin);
    let membership = Membership::from_genesis(&manifest).unwrap();
    let binder = PeerBinder::new(membership);

    // A current voter binds: the committed node, generation and key.
    let voter1 = voter_certs[0].clone();
    let bound = binder
        .bind(&[der(&voter1)], &hello(PeerRole::Voter, Some(1)))
        .unwrap();
    assert_eq!(bound.role, PeerRole::Voter);
    assert_eq!(bound.replica, Some(ReplicaId([1; 16])));
    assert_eq!(bound.incarnation, Some(ReplicaIncarnation::new(1).unwrap()));

    // A freshly issued certificate for the same node and generation,
    // with a different key, is not this voter. Without the committed
    // key, an issuer that is compromised or merely tricked mints a peer
    // of an existing cluster.
    let reissued = issuer.issue(1, 1, PeerRole::Voter, "voters", "voter-1");
    assert_ne!(reissued, voter1);
    assert_eq!(
        binder.bind(&[der(&reissued)], &hello(PeerRole::Voter, Some(1))),
        Err(BindError::IncarnationMismatch),
        "the committed key decides, not merely the committed name"
    );

    // A stale generation (an old-disk clone returning) cannot vote.
    let stale = issuer.issue(1, 5, PeerRole::Voter, "voters", "voter-1");
    assert_eq!(
        binder.bind(&[der(&stale)], &hello(PeerRole::Voter, Some(5))),
        Err(BindError::IncarnationMismatch),
        "the committed generation is 1, not 5"
    );

    // The Hello incarnation must match the certificate's.
    assert_eq!(
        binder.bind(&[der(&voter1)], &hello(PeerRole::Voter, Some(2))),
        Err(BindError::IncarnationMismatch)
    );

    // A node that is not a committed voter (node 4 is not in genesis;
    // node 9 is an observer certificate) cannot bind as a voter.
    let observer = issuer.issue(9, 1, PeerRole::Observer, "observers", "observer-0");
    assert_eq!(
        binder.bind(&[der(&observer)], &hello(PeerRole::Voter, Some(1))),
        Err(BindError::RoleNotAuthorized),
        "an observer certificate declaring voter is refused by role"
    );
    // The observer binds as an observer (peer access, never a vote).
    let bound = binder
        .bind(&[der(&observer)], &hello(PeerRole::Observer, Some(1)))
        .unwrap();
    assert_eq!(bound.role, PeerRole::Observer);
    assert_eq!(bound.replica, Some(ReplicaId([9; 16])));

    // Wrong origin: another cluster or domain.
    let mut wrong_cluster = hello(PeerRole::Voter, Some(1));
    wrong_cluster.cluster_id = ClusterId([0xff; 16]);
    assert_eq!(
        binder.bind(&[der(&voter1)], &wrong_cluster),
        Err(BindError::ClusterMismatch)
    );
    let mut wrong_domain = hello(PeerRole::Voter, Some(1));
    wrong_domain.domain_id = DomainId([0xff; 16]);
    assert_eq!(
        binder.bind(&[der(&voter1)], &wrong_domain),
        Err(BindError::DomainMismatch)
    );

    // A stranger certificate with no node URI SAN.
    let stranger = issuer.stranger();
    assert_eq!(
        binder.bind(&[der(&stranger)], &hello(PeerRole::Voter, Some(1))),
        Err(BindError::UnknownCertificate)
    );
    assert_eq!(
        binder.bind(&[], &hello(PeerRole::Voter, Some(1))),
        Err(BindError::NoCertificate)
    );

    // A cloned identity: two connections presenting the same certificate
    // bind to the same exact (node, incarnation); consensus counts that
    // identity once. The binder never invents a second identity.
    let clone_a = binder
        .bind(&[der(&voter1)], &hello(PeerRole::Voter, Some(1)))
        .unwrap();
    let clone_b = binder
        .bind(&[der(&voter1)], &hello(PeerRole::Voter, Some(1)))
        .unwrap();
    assert_eq!(clone_a.replica, clone_b.replica);
    assert_eq!(clone_a.incarnation, clone_b.incarnation);

    // A committed handoff (task-54+) swaps membership: voter-2 rolls its
    // key to generation 2; the new certificate binds, the old does not.
    let voter2_g1 = voter_certs[1].clone();
    assert!(
        binder
            .bind(&[der(&voter2_g1)], &hello(PeerRole::Voter, Some(1)))
            .is_ok()
    );
    let voter2_g2 = issuer.issue(2, 2, PeerRole::Voter, "voters", "voter-2");
    let next = Membership::from_genesis(&GenesisManifest {
        voters: vec![
            VoterSeed {
                node: hex(&[1; 16]),
                incarnation: 1,
                public_key: spki(&voter_certs[0]),
            },
            VoterSeed {
                node: hex(&[2; 16]),
                incarnation: 2,
                public_key: spki(&voter2_g2),
            },
            VoterSeed {
                node: hex(&[3; 16]),
                incarnation: 1,
                public_key: spki(&voter_certs[2]),
            },
        ],
        ..manifest.clone()
    })
    .unwrap();
    assert!(binder.install(next), "same cluster and domain");
    assert_eq!(
        binder.bind(&[der(&voter2_g1)], &hello(PeerRole::Voter, Some(1))),
        Err(BindError::IncarnationMismatch)
    );
    assert!(
        binder
            .bind(&[der(&voter2_g2)], &hello(PeerRole::Voter, Some(2)))
            .is_ok()
    );
}

#[test]
fn first_boot_establishes_the_journal_before_pinning_the_manifest() {
    // Pinning first and crashing left a pinned digest with no journal,
    // and the returning-node path then read that as a lost journal:
    // a node that had never run was quarantined and could not start.
    #[derive(Default)]
    struct CrashOnPin {
        pinned: Option<Digest32>,
        journal: bool,
    }
    impl GenesisStore for CrashOnPin {
        fn pinned_digest(&self) -> Result<Option<Digest32>, StoreFailure> {
            Ok(self.pinned)
        }
        fn pin(&mut self, _digest: Digest32) -> Result<(), StoreFailure> {
            // The power goes out exactly here.
            Err(StoreFailure)
        }
        fn journal_intact(&self) -> Result<bool, StoreFailure> {
            Ok(self.journal)
        }
        fn establish_journal(&mut self) -> Result<(), StoreFailure> {
            self.journal = true;
            Ok(())
        }
    }
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, _certs) = manifest_with_voters(&mut issuer, &admin);
    let signed = sign_genesis(&manifest, &admin.enc).unwrap();
    let verified = verify_genesis(&signed, &pinned(&admin), 1).unwrap();

    let mut store = CrashOnPin::default();
    assert_eq!(initialize(&verified, &mut store), Err(InitError::Store));
    // Nothing was pinned, and the journal is there: the retry is an
    // ordinary first boot, not a quarantine.
    assert_eq!(store.pinned, None);
    assert!(store.journal);

    let mut retry = Store {
        journal: store.journal,
        ..Store::default()
    };
    let init = initialize(&verified, &mut retry).unwrap();
    assert!(init.first_boot);
}

#[test]
fn a_binder_takes_its_origin_from_the_membership_it_holds() {
    // Cluster and domain used to be passed alongside the membership, so
    // the binder could enforce an origin the committed membership did
    // not agree with, and a handoff to another cluster's membership kept
    // the old pair rather than being refused.
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, voter_certs) = manifest_with_voters(&mut issuer, &admin);
    let membership = Membership::from_genesis(&manifest).unwrap();
    let binder = PeerBinder::new(membership);
    assert!(
        binder
            .bind(&[der(&voter_certs[0])], &hello(PeerRole::Voter, Some(1)))
            .is_ok()
    );
    // A membership of another cluster is not a handoff of this one.
    let foreign = Membership::from_genesis(&GenesisManifest {
        cluster: hex(&[0xfe; 16]),
        ..manifest.clone()
    })
    .unwrap();
    assert!(!binder.install(foreign), "another cluster is refused");
    // The binder still holds the membership it started from.
    assert!(
        binder
            .bind(&[der(&voter_certs[0])], &hello(PeerRole::Voter, Some(1)))
            .is_ok()
    );
}

// ---------------------------------------------------------------------
// task-58: the credential lifecycle. What a renewal is, what it is not,
// and what a node that has one of the other things is told.
// ---------------------------------------------------------------------

/// A renewal is the committed generation presenting the committed key,
/// and it changes nothing about membership.
///
/// Everything else -- a new key at the same generation, a later
/// generation, an earlier one -- is a named refusal rather than a bool,
/// because the interesting cases *are* the refusals and an operator who
/// cannot tell a stale clone from an early replacement cannot act on
/// either.
#[test]
fn a_renewal_keeps_the_committed_key_and_everything_else_is_named() {
    use coord_membership::CredentialChange;

    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, certs) = manifest_with_voters(&mut issuer, &admin);
    let membership = Membership::from_genesis(&manifest).unwrap();
    let node = ReplicaId([1; 16]);
    let one = ReplicaIncarnation::new(1).unwrap();
    let two = ReplicaIncarnation::new(2).unwrap();
    let committed_key = {
        let (_, x509) = x509_parser::parse_x509_certificate(&certs[0]).unwrap();
        x509.public_key().raw.to_vec()
    };

    // The committed generation with the committed key.
    assert_eq!(
        membership.classify_credential(&node, one, &committed_key),
        CredentialChange::Renewal
    );

    // A fresh leaf at the same generation carries a fresh key, because
    // the CSR does. That is not a renewal: a key change is a generation
    // change, and a generation change is committed. Without this an
    // issuer that was compromised or merely tricked mints a second key
    // for an existing voter's current generation.
    let refreshed = issuer.issue(1, 1, PeerRole::Voter, "voters", "voter-1");
    let (_, x509) = x509_parser::parse_x509_certificate(&refreshed).unwrap();
    let other_key = x509.public_key().raw.to_vec();
    assert_ne!(other_key, committed_key);
    assert_eq!(
        membership.classify_credential(&node, one, &other_key),
        CredentialChange::UncommittedKey
    );

    // A later generation: a replacement in progress. The certificate
    // may be perfectly valid; what is missing is the committed
    // configuration that makes this generation the voter.
    assert_eq!(
        membership.classify_credential(&node, two, &other_key),
        CredentialChange::RequiresCommit {
            committed: one,
            presented: two,
        }
    );

    // An earlier one: a credential from before a replacement, or a disk
    // cloned from before one.
    let later = {
        let mut manifest = manifest.clone();
        manifest.voters[0].incarnation = 2;
        manifest.voters[0].public_key = spki(&refreshed);
        Membership::from_genesis(&manifest).unwrap()
    };
    assert_eq!(
        later.classify_credential(&node, one, &committed_key),
        CredentialChange::Stale {
            committed: two,
            presented: one,
        }
    );
    // And once the replacement is committed, the same credential that
    // required a commit is an ordinary renewal.
    assert_eq!(
        later.classify_credential(&node, two, &other_key),
        CredentialChange::Renewal
    );

    // A node that is not a voter of this epoch at all.
    assert_eq!(
        membership.classify_credential(&ReplicaId([9; 16]), one, &committed_key),
        CredentialChange::NotAVoter
    );
}

/// The binder binds a renewal and refuses every other credential state,
/// telling the peer only that it was refused.
#[test]
fn the_binder_binds_a_renewal_and_tells_a_refused_peer_nothing_else() {
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, certs) = manifest_with_voters(&mut issuer, &admin);
    let membership = Membership::from_genesis(&manifest).unwrap();
    let binder = PeerBinder::new(membership);

    // The committed key binds.
    let bound = binder
        .bind(&[der(&certs[0])], &hello(PeerRole::Voter, Some(1)))
        .expect("the committed voter binds");
    assert_eq!(bound.replica, Some(ReplicaId([1; 16])));

    // A fresh leaf at the same generation does not, and neither does a
    // later generation, and neither does an earlier one. All three are
    // the same answer on the wire: which distinction it was is the node
    // operator's business, not the caller's.
    for (what, cert, incarnation) in [
        (
            "a new key at the committed generation",
            issuer.issue(1, 1, PeerRole::Voter, "voters", "voter-1"),
            1u64,
        ),
        (
            "a generation nothing committed",
            issuer.issue(1, 2, PeerRole::Voter, "voters", "voter-1"),
            2,
        ),
    ] {
        assert_eq!(
            binder.bind(&[der(&cert)], &hello(PeerRole::Voter, Some(incarnation))),
            Err(BindError::IncarnationMismatch),
            "{what} bound as the voter"
        );
    }

    // A non-voter role never binds as a voter, however current its
    // credential: an observer or a collector holds catch-up state and
    // submissions, not voting entitlement.
    for role in [PeerRole::Observer, PeerRole::Frontend, PeerRole::Learner] {
        let cert = issuer.issue(1, 1, PeerRole::Voter, "voters", "voter-1");
        assert_eq!(
            binder.bind(&[der(&cert)], &hello(role, Some(1))),
            Err(BindError::RoleNotAuthorized),
            "{role:?} bound against a voter certificate"
        );
    }
}

/// The binder says when the credential it admitted stops being valid, so
/// the transport can end the connection with it (task-58).
///
/// Certificate validation happens once, at the handshake. Without an
/// answer here a connection would outlive the credential that made it,
/// and renewal, rotation and revocation would all stop reaching the peer
/// that already got in.
#[test]
fn the_binder_reports_when_the_credential_it_admitted_ends() {
    let admin = admin_key();
    let mut issuer = build_issuer();
    let (manifest, certs) = manifest_with_voters(&mut issuer, &admin);
    let binder = PeerBinder::new(Membership::from_genesis(&manifest).unwrap());

    let chain = [der(&certs[0])];
    let expires_at = binder
        .expires_at(&chain)
        .expect("the binder knows when this leaf ends");
    use x509_parser::prelude::FromDer as _;
    let (_, leaf) = x509_parser::certificate::X509Certificate::from_der(chain[0].as_ref()).unwrap();
    assert_eq!(
        expires_at,
        u64::try_from(leaf.validity().not_after.timestamp()).unwrap(),
        "the reported deadline is not the leaf's own"
    );
    assert!(
        expires_at > NOW,
        "a certificate this fixture just issued reports an expiry in the past"
    );

    // Nothing to report about nothing: an anonymous client presents no
    // chain, and the connection is then bounded by the age cap alone.
    assert_eq!(binder.expires_at(&[]), None);
}
