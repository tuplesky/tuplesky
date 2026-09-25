//! task-41 acceptance: cold enrollment works before any voter; requests
//! are rejected on signature, algorithm, SAN/CA, lifetime and workload
//! errors; issuer outage fails closed; the CA is validated at startup
//! and its key never leaves the process; a certificate is not a vote.

use std::collections::BTreeMap;

use coord_authn::{
    ClockHealth, IssuerConfig, JwksLimits, KubernetesMode, Registry, WifVerifier, WorkloadKind,
};
use coord_node_issuer::{
    Ca, CaError, IssueError, NodeIssuer, NodeRequest, RolePolicy, parse_node_uri,
};
use coord_types::ids::{ClusterId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;
use rcgen::PublicKeyData;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};
use time::OffsetDateTime;

const AUD: &str = "node-enrollment";
const K8S_ISS: &str = "https://kubernetes.default.svc";
const CLUSTER: ClusterId = ClusterId([1; 16]);
const NOW: u64 = 1_700_000_000;

fn b64url(bytes: &[u8]) -> String {
    coord_sts::keys::b64url(bytes)
}

fn ecdsa() -> KeyPair {
    KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap()
}

/// A self-signed CA (the reference issuer's protected key) and its DER.
fn ca(now: u64, span_secs: i64) -> (Vec<u8>, Vec<u8>) {
    let key = ecdsa();
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "tuplesky node CA");
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.not_before = OffsetDateTime::from_unix_timestamp(now as i64 - 10).unwrap();
    params.not_after = OffsetDateTime::from_unix_timestamp(now as i64 + span_secs).unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert.der().to_vec(), key.serialize_der())
}

struct Idp {
    kid: String,
    key: KeyPair,
    jwks: Vec<u8>,
}

fn idp() -> Idp {
    let key = ecdsa();
    let point = key.public_key_raw();
    let jwks = serde_json::to_vec(&serde_json::json!({"keys": [{
        "kty": "EC", "crv": "P-256", "kid": "kk", "alg": "ES256", "use": "sig",
        "x": b64url(&point[1..33]), "y": b64url(&point[33..65]),
    }]}))
    .unwrap();
    Idp {
        kid: "kk".into(),
        key,
        jwks,
    }
}

fn assertion(idp: &Idp, sub: &str, ns: &str, sa: &str) -> String {
    let mut h = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    h.kid = Some(idp.kid.clone());
    let claims = serde_json::json!({
        "iss": K8S_ISS, "sub": sub, "aud": AUD, "exp": NOW + 300, "iat": NOW,
        "kubernetes.io": {"namespace": ns, "serviceaccount": {"name": sa}},
    });
    jsonwebtoken::encode(
        &h,
        &claims,
        &jsonwebtoken::EncodingKey::from_ec_der(&idp.key.serialize_der()),
    )
    .unwrap()
}

fn verifier(idp: &Idp) -> WifVerifier {
    let config = IssuerConfig {
        name: "k8s".into(),
        issuer: K8S_ISS.into(),
        jwks_url: "https://k8s/keys".into(),
        algorithms: vec![jsonwebtoken::Algorithm::ES256],
        audiences: vec![AUD.into()],
        max_age_secs: Some(3600),
        allow_insecure_loopback: false,
    };
    let mut registry = Registry::new(vec![config], JwksLimits::default()).unwrap();
    registry.install_keys("k8s", &idp.jwks, NOW).unwrap();
    let mut kinds = BTreeMap::new();
    kinds.insert(
        "k8s".to_string(),
        WorkloadKind::Kubernetes(KubernetesMode::Offline),
    );
    WifVerifier::new(registry, kinds)
}

fn node(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn policy() -> RolePolicy {
    RolePolicy {
        issuer: "k8s".into(),
        required: [("namespace", "voters"), ("serviceaccount", "voter-0")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        cluster: CLUSTER,
        nodes: vec![node(1)],
        role: PeerRole::Voter,
        min_incarnation: 2,
        max_lifetime_secs: 3600,
        dns_names: vec!["voter-0.cluster-1.internal".into()],
        ip_addresses: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 7))],
    }
}

fn csr(usages: Vec<KeyUsagePurpose>, is_ca: IsCa) -> (Vec<u8>, KeyPair) {
    let key = ecdsa();
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "requested-by-node");
    params.key_usages = usages;
    params.is_ca = is_ca;
    // A node may request its own DNS SAN; the issuer ignores it and uses
    // the policy's names.
    params.subject_alt_names = vec![SanType::DnsName(
        Ia5String::try_from("attacker.example").unwrap(),
    )];
    let csr = params.serialize_request(&key).unwrap();
    (csr.der().to_vec(), key)
}

fn issuer(idp: &Idp) -> NodeIssuer {
    let (cert, key) = ca(NOW, 86_400);
    let ca = Ca::load(&cert, &key, NOW).unwrap();
    NodeIssuer::new(verifier(idp), ca, vec![policy()])
}

fn request(idp: &Idp, csr_der: Vec<u8>, node: u8, incarnation: u64, lifetime: u64) -> NodeRequest {
    NodeRequest {
        assertion: assertion(
            idp,
            "system:serviceaccount:voters:voter-0",
            "voters",
            "voter-0",
        ),
        csr_der,
        node: [node; 16],
        incarnation,
        lifetime_secs: lifetime,
    }
}

fn clock(now: u64) -> ClockHealth {
    ClockHealth::healthy(now, 5)
}

#[test]
fn cold_enrollment_works_before_any_voter_and_binds_the_node_identity() {
    let provider = idp();
    let mut issuer = issuer(&provider);
    let (csr_der, node_key) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    let issued = issuer
        .enroll(&request(&provider, csr_der, 1, 2, 300), &clock(NOW))
        .unwrap();
    // The certificate ends with the assertion that authorized it, read
    // conservatively under the clock's uncertainty, rather than running
    // for the policy's whole lifetime.
    assert_eq!(issued.expires_at, NOW + 295);
    let identity = parse_node_uri(&issued.node_uri).unwrap();
    assert_eq!(identity.cluster, CLUSTER);
    assert_eq!(identity.node, node(1));
    assert_eq!(identity.incarnation, ReplicaIncarnation::new(2).unwrap());
    assert_eq!(identity.role, PeerRole::Voter);
    // The certificate carries the policy's node URI, DNS and address, not the
    // CSR's attacker SAN, and it is not a CA.
    let (_, x509) = x509_parser::parse_x509_certificate(&issued.certificate).unwrap();
    assert!(!x509.is_ca());
    let sans: Vec<String> = x509
        .subject_alternative_name()
        .unwrap()
        .unwrap()
        .value
        .general_names
        .iter()
        .map(|g| format!("{g:?}"))
        .collect();
    assert!(
        sans.iter()
            .any(|s| s.contains("voter-0.cluster-1.internal"))
    );
    assert!(sans.iter().any(|s| s.contains(&issued.node_uri)));
    // And the policy's address, for a peer that dials the node by IP
    // literal and checks the certificate's IP SANs rather than its names.
    assert!(
        x509.subject_alternative_name()
            .unwrap()
            .unwrap()
            .value
            .general_names
            .iter()
            .any(|g| matches!(
                g,
                x509_parser::extensions::GeneralName::IPAddress(ip) if *ip == [10, 0, 0, 7]
            )),
        "{sans:?}"
    );
    assert!(
        !sans.iter().any(|s| s.contains("attacker.example")),
        "CSR SAN not copied"
    );
    // The certificate's public key is the node's, proving possession bound.
    assert_eq!(
        x509.public_key().raw,
        node_key.subject_public_key_info().as_slice()
    );
    // The CA certificate is served for bootstrap; the CA key never leaves.
    assert!(!issuer.ca().certificate().is_empty());
    assert_eq!(issuer.issued, 1);
}

#[test]
fn requests_are_rejected_on_signature_algorithm_san_ca_lifetime_and_workload() {
    let provider = idp();
    let mut issuer = issuer(&provider);
    // A CA request is refused.
    let (ca_csr, _) = csr(
        vec![KeyUsagePurpose::KeyCertSign],
        IsCa::Ca(BasicConstraints::Unconstrained),
    );
    assert_eq!(
        issuer.enroll(&request(&provider, ca_csr, 1, 2, 300), &clock(NOW)),
        Err(IssueError::CaRequested)
    );
    // Certificate-signing usage without the CA bit is still forbidden.
    let (sign_csr, _) = csr(vec![KeyUsagePurpose::KeyCertSign], IsCa::NoCa);
    assert_eq!(
        issuer.enroll(&request(&provider, sign_csr, 1, 2, 300), &clock(NOW)),
        Err(IssueError::ForbiddenUsage)
    );
    // A corrupt CSR (proof of possession) is refused.
    let (mut csr_der, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    let n = csr_der.len();
    csr_der[n - 1] ^= 0xff;
    assert_eq!(
        issuer.enroll(&request(&provider, csr_der, 1, 2, 300), &clock(NOW)),
        Err(IssueError::Csr)
    );
    // An unauthorized node identity.
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    assert_eq!(
        issuer.enroll(&request(&provider, ok_csr, 2, 2, 300), &clock(NOW)),
        Err(IssueError::Policy(
            coord_node_issuer::PolicyError::NodeNotAuthorized
        ))
    );
    // A rolled-back incarnation below the floor.
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    assert_eq!(
        issuer.enroll(&request(&provider, ok_csr, 1, 1, 300), &clock(NOW)),
        Err(IssueError::Policy(
            coord_node_issuer::PolicyError::IncarnationTooOld
        ))
    );
    // An overlong lifetime.
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    assert_eq!(
        issuer.enroll(&request(&provider, ok_csr, 1, 2, 999_999), &clock(NOW)),
        Err(IssueError::Policy(
            coord_node_issuer::PolicyError::LifetimeTooLong
        ))
    );
    // A workload the policy does not authorize (wrong service account).
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    let mut req = request(&provider, ok_csr, 1, 2, 300);
    req.assertion = assertion(
        &provider,
        "system:serviceaccount:voters:other",
        "voters",
        "other",
    );
    assert_eq!(
        issuer.enroll(&req, &clock(NOW)),
        Err(IssueError::Policy(coord_node_issuer::PolicyError::NoRule))
    );
    // An assertion signed by the wrong key (algorithm/signature).
    let other = idp();
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    let mut req = request(&provider, ok_csr, 1, 2, 300);
    req.assertion = assertion(
        &other,
        "system:serviceaccount:voters:voter-0",
        "voters",
        "voter-0",
    );
    assert_eq!(issuer.enroll(&req, &clock(NOW)), Err(IssueError::Assertion));
    assert_eq!(issuer.issued, 0, "nothing was issued");
}

#[test]
fn issuer_outage_and_bad_ca_fail_closed() {
    let provider = idp();
    // Keys not yet installed: enrollment reports the fetch condition, not
    // a certificate.
    let config = IssuerConfig {
        name: "k8s".into(),
        issuer: K8S_ISS.into(),
        jwks_url: "https://k8s/keys".into(),
        algorithms: vec![jsonwebtoken::Algorithm::ES256],
        audiences: vec![AUD.into()],
        max_age_secs: Some(3600),
        allow_insecure_loopback: false,
    };
    let registry = Registry::new(vec![config], JwksLimits::default()).unwrap();
    let mut kinds = BTreeMap::new();
    kinds.insert(
        "k8s".to_string(),
        WorkloadKind::Kubernetes(KubernetesMode::Offline),
    );
    let (cert, key) = ca(NOW, 86_400);
    let mut dry = NodeIssuer::new(
        WifVerifier::new(registry, kinds),
        Ca::load(&cert, &key, NOW).unwrap(),
        vec![policy()],
    );
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    assert!(matches!(
        dry.enroll(&request(&provider, ok_csr, 1, 2, 300), &clock(NOW)),
        Err(IssueError::KeysUnavailable { .. })
    ));
    // An unhealthy clock cannot establish validity.
    let mut issuer = issuer(&provider);
    let (ok_csr, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    let sick = ClockHealth {
        now: NOW,
        uncertainty: 5,
        healthy: false,
    };
    assert_eq!(
        issuer.enroll(&request(&provider, ok_csr, 1, 2, 300), &sick),
        Err(IssueError::ClockUnhealthy)
    );
    // CA validation at startup: a non-CA certificate, an expired CA and a
    // mismatched key are all refused (fresh keypairs; the shadowing
    // `idp` binding above is not called again).
    let leaf_key = ecdsa();
    let mut leaf = CertificateParams::default();
    leaf.distinguished_name.push(DnType::CommonName, "not a ca");
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.not_before = OffsetDateTime::from_unix_timestamp(NOW as i64 - 10).unwrap();
    leaf.not_after = OffsetDateTime::from_unix_timestamp(NOW as i64 + 100).unwrap();
    let leaf_cert = leaf.self_signed(&leaf_key).unwrap();
    assert_eq!(
        Ca::load(leaf_cert.der(), &leaf_key.serialize_der(), NOW).err(),
        Some(CaError::NotCa)
    );
    // A CA certificate without keyCertSign usage cannot sign.
    let signer_key = ecdsa();
    let mut no_sign = CertificateParams::default();
    no_sign
        .distinguished_name
        .push(DnType::CommonName, "ca without sign");
    no_sign.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    no_sign.key_usages = vec![KeyUsagePurpose::CrlSign];
    no_sign.not_before = OffsetDateTime::from_unix_timestamp(NOW as i64 - 10).unwrap();
    no_sign.not_after = OffsetDateTime::from_unix_timestamp(NOW as i64 + 100).unwrap();
    let no_sign_cert = no_sign.self_signed(&signer_key).unwrap();
    assert_eq!(
        Ca::load(no_sign_cert.der(), &signer_key.serialize_der(), NOW).err(),
        Some(CaError::CannotSign)
    );
    let (expired_cert, expired_key) = ca(NOW - 10_000, 1_000);
    assert_eq!(
        Ca::load(&expired_cert, &expired_key, NOW).err(),
        Some(CaError::Expired)
    );
    let (cert, _) = ca(NOW, 86_400);
    let wrong_key = ecdsa();
    assert_eq!(
        Ca::load(&cert, &wrong_key.serialize_der(), NOW).err(),
        Some(CaError::KeyMismatch)
    );
    assert!(Ca::load(b"not a cert", &leaf_key.serialize_der(), NOW).is_err());
}

#[test]
fn a_certificate_outlives_neither_its_assertion_nor_the_ca() {
    // Validity used to be the policy's lifetime measured from now, so a
    // node presenting a credential good for a minute walked away with a
    // certificate good for the policy's whole lifetime, and a CA checked
    // only at startup went on signing past its own expiry.
    let provider = idp();
    let mut issuer = issuer(&provider);
    let (csr_der, _) = csr(vec![KeyUsagePurpose::DigitalSignature], IsCa::NoCa);
    // A lifetime the policy allows, but longer than the assertion has
    // left to live at this point.
    let issued = issuer
        .enroll(&request(&provider, csr_der, 1, 2, 300), &clock(NOW + 200))
        .unwrap();
    assert_eq!(
        issued.expires_at,
        NOW + 295,
        "the assertion's conservative deadline decides"
    );
    // The leaf starts early enough for a peer whose clock reads behind
    // this one to accept it.
    let (_, x509) = x509_parser::parse_x509_certificate(&issued.certificate).unwrap();
    assert_eq!(
        x509.validity().not_before.timestamp(),
        (NOW + 200 - 5) as i64,
        "not_before allows for the clock's uncertainty"
    );
    assert_eq!(x509.validity().not_after.timestamp(), (NOW + 295) as i64);
}
