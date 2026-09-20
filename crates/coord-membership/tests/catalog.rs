//! Verifying an endpoint catalog against a committed membership
//! (task-j08).
//!
//! A daemon holds one epoch's committed configuration. It does not hold
//! the configuration chain, and building one needs a genesis anchor and
//! a signed root record -- machinery that belongs to the discovery
//! workstream and that a node should not have to assemble before it can
//! find its peers.
//!
//! So the catalog's evidence rules are anchored on the committed voter
//! set rather than on the chain, and these tests hold that the anchor is
//! all that changed: the same rules, the same refusals, from the
//! authority a daemon already has.

use coord_membership::configuration::{CatalogError, EvidenceError, sign_message};
use coord_membership::genesis::{GenesisManifest, VoterSeed, b64url, hex_id};
use coord_membership::membership::Membership;
use coord_types::config_v1::{EndpointCatalogV1, EndpointV1, VoterSignatureV1};
use coord_types::identity::Digest32;
use coord_types::ids::{
    ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration, ReplicaId, ReplicaIncarnation,
};
use jsonwebtoken::EncodingKey;
use rcgen::KeyPair;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn node(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

/// A voter and the key it proves with.
struct Voter {
    id: ReplicaId,
    incarnation: ReplicaIncarnation,
    key: KeyPair,
}

impl Voter {
    fn new(n: u8) -> Self {
        Voter {
            id: node(n),
            incarnation: inc(u64::from(n)),
            key: KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap(),
        }
    }

    /// The `SubjectPublicKeyInfo` a TLS peer presents for this key, as
    /// genesis commits it -- taken from a real certificate rather than
    /// re-encoded, so the bytes are the ones a binder would compare.
    fn spki(&self) -> Vec<u8> {
        use x509_parser::prelude::FromDer;
        let params =
            rcgen::CertificateParams::new(vec!["voter.example".to_owned()]).expect("params");
        let cert = params.self_signed(&self.key).expect("self-signed");
        let (_, x509) =
            x509_parser::certificate::X509Certificate::from_der(cert.der()).expect("parseable");
        x509.public_key().raw.to_vec()
    }

    fn sign(&self, message: &Digest32) -> VoterSignatureV1 {
        VoterSignatureV1 {
            node: self.id,
            incarnation: self.incarnation,
            signature: sign_message(
                &EncodingKey::from_ec_der(&self.key.serialize_der()),
                message,
            )
            .unwrap(),
        }
    }
}

/// A genesis manifest committing each voter's `SubjectPublicKeyInfo`.
///
/// That is the encoding a TLS peer presents and the one the peer binder
/// compares a certificate against. The catalog's signature wants the
/// uncompressed point inside it, and the membership derives that from
/// the same committed bytes -- which is the point of the test below.
fn membership(voters: &[&Voter], domain: DomainId) -> Membership {
    let manifest = GenesisManifest {
        cluster: hex_id(&CLUSTER.0),
        domain: hex_id(&domain.0),
        epoch: 1,
        voters: voters
            .iter()
            .map(|v| VoterSeed {
                node: hex_id(&v.id.0),
                incarnation: v.incarnation.get(),
                public_key: b64url(&v.spki()),
            })
            .collect(),
        issuer_roots: vec!["cm9vdA".to_owned()],
        wif_rules: vec![serde_json::json!({ "issuer": "test" })],
        admin: hex_id(&[9; 16]),
        protocol_version: 1,
    };
    Membership::from_genesis(&manifest).expect("membership")
}

fn catalog(
    epoch: u64,
    domain: DomainId,
    entries: &[(&Voter, ReplicaIncarnation)],
    attester: &Voter,
) -> EndpointCatalogV1 {
    let mut endpoints: Vec<EndpointV1> = entries
        .iter()
        .map(|(v, incarnation)| EndpointV1 {
            node: v.id,
            incarnation: *incarnation,
            addresses: vec![format!("{}.example:7443", v.id.0[0])],
            certificate_fingerprint: None,
        })
        .collect();
    endpoints.sort_by(|a, b| a.node.cmp(&b.node));
    let mut c = EndpointCatalogV1 {
        cluster: CLUSTER,
        domain,
        epoch: ConfigurationEpoch::new(epoch).unwrap(),
        generation: EndpointGeneration::new(1).unwrap(),
        endpoints,
        attestation: VoterSignatureV1 {
            node: attester.id,
            incarnation: attester.incarnation,
            signature: vec![0; 64],
        },
    };
    c.attestation = attester.sign(&c.catalog_message());
    c
}

/// A committed membership is enough to verify a catalog, and the
/// signature verifies against the key genesis committed -- whichever way
/// that key was written down.
#[test]
fn a_committed_membership_verifies_a_catalog_its_own_voter_attested() {
    let one = Voter::new(1);
    let two = Voter::new(2);
    let membership = membership(&[&one, &two], DOMAIN);

    let catalog = catalog(
        1,
        DOMAIN,
        &[(&one, one.incarnation), (&two, two.incarnation)],
        &one,
    );

    assert_eq!(membership.verify_endpoints(&catalog), Ok(()));
    assert_eq!(
        catalog.endpoints.len(),
        2,
        "both voters' addresses are in it"
    );
}

/// The attestation is evidence, not decoration. A signature by a key
/// this configuration did not commit to is refused, and so is one by a
/// node it does not know.
#[test]
fn a_catalog_no_committed_voter_signed_is_refused() {
    let one = Voter::new(1);
    let two = Voter::new(2);
    let membership = membership(&[&one, &two], DOMAIN);

    // A stranger's signature.
    let stranger = Voter::new(9);
    let mut forged = catalog(1, DOMAIN, &[(&one, one.incarnation)], &one);
    forged.attestation = stranger.sign(&forged.catalog_message());
    assert_eq!(
        membership.verify_endpoints(&forged),
        Err(CatalogError::Evidence(EvidenceError::NotVoter {
            node: stranger.id
        }))
    );

    // A committed voter's name over somebody else's key.
    let mut impersonated = catalog(1, DOMAIN, &[(&one, one.incarnation)], &one);
    impersonated.attestation = VoterSignatureV1 {
        node: two.id,
        incarnation: two.incarnation,
        ..stranger.sign(&impersonated.catalog_message())
    };
    assert_eq!(
        membership.verify_endpoints(&impersonated),
        Err(CatalogError::Evidence(EvidenceError::Signature {
            node: two.id
        }))
    );

    // A committed voter's own signature over different content.
    let mut tampered = catalog(1, DOMAIN, &[(&one, one.incarnation)], &one);
    tampered.endpoints[0].addresses = vec!["elsewhere.example:7443".to_owned()];
    assert_eq!(
        membership.verify_endpoints(&tampered),
        Err(CatalogError::Evidence(EvidenceError::Signature {
            node: one.id
        }))
    );
}

/// A catalog cannot introduce a voter, re-incarnate one, or belong to
/// another cluster, domain or epoch. It carries addresses and nothing
/// else that matters.
#[test]
fn a_catalog_never_says_who_the_voters_are() {
    let one = Voter::new(1);
    let two = Voter::new(2);
    let stranger = Voter::new(9);
    let membership = membership(&[&one, &two], DOMAIN);

    let introduces = catalog(
        1,
        DOMAIN,
        &[(&one, one.incarnation), (&stranger, stranger.incarnation)],
        &one,
    );
    assert_eq!(
        membership.verify_endpoints(&introduces),
        Err(CatalogError::NotVoter { node: stranger.id })
    );

    let reincarnates = catalog(1, DOMAIN, &[(&two, inc(99))], &one);
    assert_eq!(
        membership.verify_endpoints(&reincarnates),
        Err(CatalogError::WrongIncarnation { node: two.id })
    );

    let elsewhere = catalog(1, DomainId([0x33; 16]), &[(&one, one.incarnation)], &one);
    assert_eq!(
        membership.verify_endpoints(&elsewhere),
        Err(CatalogError::OriginMismatch)
    );

    let later = catalog(2, DOMAIN, &[(&one, one.incarnation)], &one);
    assert_eq!(
        membership.verify_endpoints(&later),
        Err(CatalogError::UnknownEpoch),
        "a catalog never introduces an epoch, whatever anchors it"
    );
}
