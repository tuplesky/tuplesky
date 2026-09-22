//! The configuration chain with real ES256 keys: genesis root, handoffs
//! 3->5 and 5->3, and every way a record, ballot, catalog or hint must be
//! refused. No issuer or certificate authority is involved anywhere:
//! every key the chain needs is in the records.

use std::collections::BTreeMap;

use coord_consensus::quorum::ConfigurationError;
use coord_membership::configuration::{
    BallotError, CatalogError, ChainError, ClientConfiguration, ConfigurationChain, EvidenceError,
    GenesisAnchor, HintDecision, Installed, sign_message, verify_signature,
};
use coord_membership::genesis::{
    GenesisManifest, SignedGenesis, VoterSeed, b64url, hex_id, sign_genesis, verify_genesis,
};
use coord_types::config_v1::{
    ActivationEvidenceV1, BallotConfigurationV1, BootstrapResponseV1, ConfigurationHintV1,
    EndpointCatalogV1, EndpointV1, GroupConfigurationV1, NO_PREVIOUS_CERTIFICATE,
    ObserverCatalogV1, ObserverEntryV1, QuorumPolicyId, VoterRecordV1, VoterSignatureV1,
};
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, CatalogGeneration, ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration,
    ReplicaId, ReplicaIncarnation,
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use rcgen::KeyPair;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);
const PROTOCOL: u32 = 1;

fn node(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

/// A node with its key: the only thing a node ever proves with.
struct Node {
    id: ReplicaId,
    incarnation: ReplicaIncarnation,
    key: KeyPair,
}

impl Node {
    fn new(n: u8, incarnation: u64) -> Self {
        Node {
            id: node(n),
            incarnation: inc(incarnation),
            key: KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap(),
        }
    }
    fn signing(&self) -> EncodingKey {
        EncodingKey::from_ec_der(&self.key.serialize_der())
    }
    fn record(&self) -> VoterRecordV1 {
        VoterRecordV1 {
            node: self.id,
            incarnation: self.incarnation,
            public_key: self.key.public_key_raw().to_vec(),
        }
    }
    fn sign(&self, message: &Digest32) -> VoterSignatureV1 {
        VoterSignatureV1 {
            node: self.id,
            incarnation: self.incarnation,
            signature: sign_message(&self.signing(), message).unwrap(),
        }
    }
}

struct Admin {
    enc: EncodingKey,
    dec: DecodingKey,
}

fn admin() -> Admin {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    Admin {
        enc: EncodingKey::from_ec_der(&key.serialize_der()),
        dec: DecodingKey::from_ec_der(key.public_key_raw()),
    }
}

fn manifest(cluster: ClusterId, domain: DomainId, voters: &[&Node]) -> GenesisManifest {
    GenesisManifest {
        cluster: hex_id(&cluster.0),
        domain: hex_id(&domain.0),
        epoch: 1,
        voters: voters
            .iter()
            .map(|v| VoterSeed {
                node: hex_id(&v.id.0),
                incarnation: v.incarnation.get(),
                public_key: b64url(v.key.public_key_raw()),
            })
            .collect(),
        issuer_roots: vec!["cm9vdA".to_owned()],
        wif_rules: vec![serde_json::json!({
            "issuer": "k8s",
            "namespace": "voters",
            "serviceaccount": "voter-1",
            "scope_ceiling": 7,
        })],
        admin: hex_id(&[9; 16]),
        protocol_version: PROTOCOL,
    }
}

fn sorted(voters: &[&Node]) -> Vec<VoterRecordV1> {
    let mut out: Vec<VoterRecordV1> = voters.iter().map(|v| v.record()).collect();
    out.sort_by(|a, b| a.node.cmp(&b.node));
    out
}

fn root_record(
    admin: &Admin,
    cluster: ClusterId,
    domain: DomainId,
    manifest: &GenesisManifest,
    voters: &[&Node],
) -> GroupConfigurationV1 {
    let mut record = GroupConfigurationV1 {
        cluster,
        domain,
        epoch: epoch(1),
        voters: sorted(voters),
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        previous_certificate: NO_PREVIOUS_CERTIFICATE,
        activation: ActivationEvidenceV1::Genesis {
            manifest_digest: manifest.digest(),
            admin_signature: vec![0; 64],
        },
    };
    let message = record.activation_message();
    record.activation = ActivationEvidenceV1::Genesis {
        manifest_digest: manifest.digest(),
        admin_signature: sign_message(&admin.enc, &message).unwrap(),
    };
    record
}

fn handoff_record(
    previous: &GroupConfigurationV1,
    new_voters: &[&Node],
    approvers: &[&Node],
) -> GroupConfigurationV1 {
    let mut record = GroupConfigurationV1 {
        cluster: previous.cluster,
        domain: previous.domain,
        epoch: previous.epoch.checked_next().unwrap(),
        voters: sorted(new_voters),
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        previous_certificate: previous.certificate_hash(),
        activation: ActivationEvidenceV1::Handoff {
            old_epoch: previous.epoch,
            terminal_certificate: Digest32([0x7e; 32]),
            approvals: vec![],
        },
    };
    let message = record.activation_message();
    record.activation = ActivationEvidenceV1::Handoff {
        old_epoch: previous.epoch,
        terminal_certificate: Digest32([0x7e; 32]),
        approvals: approvers.iter().map(|a| a.sign(&message)).collect(),
    };
    record
}

/// A ballot certificate for the configuration `record`: the promises are
/// made for that record's cluster, domain and certificate hash, which is
/// what a verifier compares against its own chain.
fn ballot_record(
    record: &GroupConfigurationV1,
    number: u64,
    leader: &Node,
    fast: &[&Node],
    promisers: &[&Node],
) -> BallotConfigurationV1 {
    ballot_for(
        record.cluster,
        record.domain,
        record.epoch,
        record.certificate_hash(),
        number,
        leader,
        fast,
        promisers,
    )
}

/// A ballot certificate for a context named field by field, so a test can
/// state one that no chain holds.
#[expect(clippy::too_many_arguments, reason = "every field is the context")]
fn ballot_for(
    cluster: ClusterId,
    domain: DomainId,
    epoch_no: ConfigurationEpoch,
    certificate: Digest32,
    number: u64,
    leader: &Node,
    fast: &[&Node],
    promisers: &[&Node],
) -> BallotConfigurationV1 {
    let mut fast_set: Vec<ReplicaId> = fast.iter().map(|n| n.id).collect();
    fast_set.sort();
    let mut b = BallotConfigurationV1 {
        cluster,
        domain,
        epoch: epoch_no,
        configuration_certificate: certificate,
        ballot: Ballot {
            epoch: epoch_no,
            number,
            leader: leader.id,
        },
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        fast_set,
        promises: vec![],
    };
    let message = b.ballot_message();
    b.promises = promisers.iter().map(|p| p.sign(&message)).collect();
    b
}

fn endpoint_catalog(
    epoch_no: u64,
    generation: u64,
    nodes: &[(&Node, ReplicaIncarnation)],
    attester: &Node,
) -> EndpointCatalogV1 {
    let mut endpoints: Vec<EndpointV1> = nodes
        .iter()
        .map(|(n, incarnation)| EndpointV1 {
            node: n.id,
            incarnation: *incarnation,
            addresses: vec![format!("{}.example:7443", n.id.0[0])],
            certificate_fingerprint: Some(Digest32([generation as u8; 32])),
        })
        .collect();
    endpoints.sort_by(|a, b| a.node.cmp(&b.node));
    let mut c = EndpointCatalogV1 {
        cluster: CLUSTER,
        domain: DOMAIN,
        epoch: epoch(epoch_no),
        generation: EndpointGeneration::new(generation).unwrap(),
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

fn observer_catalog(
    epoch_no: u64,
    generation: u64,
    nodes: &[&Node],
    attester: &Node,
) -> ObserverCatalogV1 {
    let mut observers: Vec<ObserverEntryV1> = nodes
        .iter()
        .map(|n| ObserverEntryV1 {
            node: n.id,
            incarnation: n.incarnation,
            region: "eu".to_owned(),
            addresses: vec![format!("{}.example:7443", n.id.0[0])],
            capabilities: 1,
        })
        .collect();
    observers.sort_by(|a, b| a.node.cmp(&b.node));
    let mut c = ObserverCatalogV1 {
        cluster: CLUSTER,
        domain: DOMAIN,
        epoch: epoch(epoch_no),
        generation: CatalogGeneration::new(generation).unwrap(),
        observers,
        attestation: VoterSignatureV1 {
            node: attester.id,
            incarnation: attester.incarnation,
            signature: vec![0; 64],
        },
    };
    c.attestation = attester.sign(&c.catalog_message());
    c
}

/// A world: five voters (1-3 initial), two observers, an admin.
struct World {
    admin: Admin,
    nodes: BTreeMap<u8, Node>,
    anchor: GenesisAnchor,
    root: GroupConfigurationV1,
}

impl World {
    fn new() -> Self {
        let admin = admin();
        let mut nodes = BTreeMap::new();
        for n in 1..=7u8 {
            nodes.insert(n, Node::new(n, 1));
        }
        let initial = [&nodes[&1], &nodes[&2], &nodes[&3]];
        let manifest = manifest(CLUSTER, DOMAIN, &initial);
        // The manifest is what deployment trust delivers: verify it the
        // production way before anchoring.
        let signed: SignedGenesis = sign_genesis(&manifest, &admin.enc).unwrap();
        let verified = verify_genesis(&signed, &admin.dec, PROTOCOL).unwrap();
        let anchor = GenesisAnchor::new(&verified, admin.dec.clone()).unwrap();
        let root = root_record(&admin, CLUSTER, DOMAIN, &verified, &initial);
        World {
            admin,
            nodes,
            anchor,
            root,
        }
    }
    fn n(&self, n: u8) -> &Node {
        &self.nodes[&n]
    }
    fn chain(&self) -> ConfigurationChain {
        ConfigurationChain::from_genesis(&self.anchor, self.root.clone()).unwrap()
    }
    /// A second, independently anchored domain over the very same voters,
    /// incarnations and keys, with the same admin and the same quorum
    /// policy: the deployment shape in which nothing but the signed
    /// context tells one domain's evidence from the other's.
    fn sibling(&self, cluster: ClusterId, domain: DomainId) -> Sibling {
        let initial = [self.n(1), self.n(2), self.n(3)];
        let manifest = manifest(cluster, domain, &initial);
        let signed: SignedGenesis = sign_genesis(&manifest, &self.admin.enc).unwrap();
        let verified = verify_genesis(&signed, &self.admin.dec, PROTOCOL).unwrap();
        Sibling {
            anchor: GenesisAnchor::new(&verified, self.admin.dec.clone()).unwrap(),
            root: root_record(&self.admin, cluster, domain, &verified, &initial),
        }
    }
}

/// Another domain's trusted root and epoch-one record.
struct Sibling {
    anchor: GenesisAnchor,
    root: GroupConfigurationV1,
}

#[test]
fn signatures_verify_only_with_the_recorded_key() {
    let a = Node::new(1, 1);
    let b = Node::new(2, 1);
    let m = Digest32([5; 32]);
    let s = sign_message(&a.signing(), &m).unwrap();
    assert_eq!(s.len(), 64);
    assert!(verify_signature(a.key.public_key_raw(), &m, &s));
    assert!(!verify_signature(b.key.public_key_raw(), &m, &s));
    assert!(!verify_signature(
        a.key.public_key_raw(),
        &Digest32([6; 32]),
        &s
    ));
    let mut t = s.clone();
    t[10] ^= 1;
    assert!(!verify_signature(a.key.public_key_raw(), &m, &t));
    assert!(!verify_signature(&a.key.public_key_raw()[1..], &m, &s));
    assert!(!verify_signature(a.key.public_key_raw(), &m, &s[..63]));
}

#[test]
fn chain_grows_only_through_old_quorum_approval_and_keeps_history() {
    let w = World::new();
    let mut chain = w.chain();
    assert_eq!(chain.current().epoch(), epoch(1));
    assert_eq!(chain.current().voters().len(), 3);

    // 3 -> 5: approved by two of the three old voters.
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    let e2 = handoff_record(&w.root, &five, &[w.n(1), w.n(3)]);
    chain.extend(e2.clone()).unwrap();
    assert_eq!(chain.current().epoch(), epoch(2));
    assert_eq!(chain.current().voters().len(), 5);
    assert_eq!(chain.current().certificate(), e2.certificate_hash());

    // 5 -> 3 with node 1 re-incarnated: approved by three of five.
    let one_v2 = Node::new(1, 2);
    let three = [&one_v2, w.n(6), w.n(7)];
    let e3 = handoff_record(&e2, &three, &[w.n(2), w.n(4), w.n(5)]);
    chain.extend(e3.clone()).unwrap();
    assert_eq!(chain.len(), 3);
    assert!(chain.current().is_voter(&node(1), inc(2)));
    assert!(!chain.current().is_voter(&node(1), inc(1)));
    assert!(!chain.current().is_voter(&node(2), inc(1)));
    // History stays: epoch 1 still answers with its own voters.
    assert!(chain.at(epoch(1)).unwrap().is_voter(&node(2), inc(1)));
    assert_eq!(chain.at(epoch(4)), None);

    // Bootstrap paging over the chain.
    let (all, complete) = chain.records_after(None, None, 64);
    assert_eq!(all.len(), 3);
    assert!(complete);
    let (one, complete) = chain.records_after(None, None, 1);
    assert_eq!(one[0].epoch, epoch(1));
    assert!(!complete);
    let (after1, complete) =
        chain.records_after(Some(epoch(1)), Some(w.root.certificate_hash()), 64);
    assert_eq!(
        after1.iter().map(|r| r.epoch.get()).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(complete);
    let (none, complete) = chain.records_after(Some(epoch(3)), Some(e3.certificate_hash()), 64);
    assert!(none.is_empty() && complete);
    let (none, complete) = chain.records_after(Some(epoch(3)), None, 64);
    assert!(none.is_empty() && complete);
    let (none, complete) = chain.records_after(Some(epoch(9)), Some(Digest32([0; 32])), 64);
    assert!(none.is_empty() && !complete);
    // A client that holds a known epoch under another certificate has
    // diverged from this chain: the answer starts at that epoch, so the
    // held record reaches the client as evidence instead of an empty
    // "you are current".
    let (diverged, complete) = chain.records_after(Some(epoch(2)), Some(Digest32([0; 32])), 64);
    assert_eq!(
        diverged.iter().map(|r| r.epoch.get()).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(complete);
    let (diverged, complete) = chain.records_after(Some(epoch(3)), Some(Digest32([0; 32])), 64);
    assert_eq!(diverged, vec![e3.clone()]);
    assert!(complete);
}

#[test]
fn every_fabrication_is_rejected() {
    let w = World::new();
    let chain = w.chain();
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    let good = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);

    let mut cases: Vec<(&str, GroupConfigurationV1, ChainError)> = Vec::new();
    // A fabricated larger epoch, internally consistent, approvals and all.
    let mut skip = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    skip.epoch = epoch(3);
    if let ActivationEvidenceV1::Handoff { old_epoch, .. } = &mut skip.activation {
        *old_epoch = epoch(2);
    }
    skip = resign_handoff(skip, &[w.n(1), w.n(2)]);
    cases.push((
        "larger epoch",
        skip,
        ChainError::EpochNotNext {
            expected: epoch(2),
            found: epoch(3),
        },
    ));
    // A larger epoch that still names epoch 1 as old: malformed.
    let mut inconsistent = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    inconsistent.epoch = epoch(3);
    cases.push((
        "inconsistent epoch",
        inconsistent,
        ChainError::Shape(coord_types::config_v1::ConfigError::OldEpochNotPrevious),
    ));
    // Approved by the new voters instead of the old ones.
    cases.push((
        "self-approved",
        handoff_record(&w.root, &five, &[w.n(4), w.n(5)]),
        ChainError::ApprovalNotVoter { node: node(4) },
    ));
    // An observer's approval.
    cases.push((
        "observer approval",
        handoff_record(&w.root, &five, &[w.n(1), w.n(6)]),
        ChainError::ApprovalNotVoter { node: node(6) },
    ));
    // An old voter under a wrong incarnation (a fresh key).
    let one_v2 = Node::new(1, 2);
    cases.push((
        "wrong incarnation",
        handoff_record(&w.root, &five, &[&one_v2, w.n(2)]),
        ChainError::ApprovalWrongIncarnation { node: node(1) },
    ));
    // The right identity but the wrong key: signature fails.
    let one_forged = Node {
        id: node(1),
        incarnation: inc(1),
        key: KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap(),
    };
    cases.push((
        "forged key",
        handoff_record(&w.root, &five, &[&one_forged, w.n(2)]),
        ChainError::ApprovalSignature { node: node(1) },
    ));
    // A minority.
    cases.push((
        "minority",
        handoff_record(&w.root, &five, &[w.n(1)]),
        ChainError::InsufficientApprovals { have: 1, need: 2 },
    ));
    // Not linked to the current certificate.
    let mut unlinked = good.clone();
    unlinked.previous_certificate = Digest32([0x11; 32]);
    unlinked = resign_handoff(unlinked, &[w.n(1), w.n(2)]);
    cases.push((
        "unlinked",
        unlinked,
        ChainError::PreviousCertificateMismatch,
    ));
    // Approvals signed over a different record (tampered after signing).
    let mut tampered = good.clone();
    tampered.voters.pop();
    cases.push((
        "tampered",
        tampered,
        ChainError::ApprovalSignature { node: node(1) },
    ));
    // Another domain.
    let mut foreign = good.clone();
    foreign.domain = DomainId([3; 16]);
    cases.push(("foreign domain", foreign, ChainError::DomainMismatch));
    // Genesis evidence at a later epoch.
    let mut regenesis = good.clone();
    regenesis.previous_certificate = NO_PREVIOUS_CERTIFICATE;
    regenesis.activation = w.root.activation.clone();
    cases.push(("re-genesis", regenesis, ChainError::EvidenceKind));

    for (name, record, expected) in cases {
        let mut c = chain.clone();
        assert_eq!(c.extend(record), Err(expected), "{name}");
        assert_eq!(c.current().epoch(), epoch(1), "{name} changed the chain");
    }
    // The good record still extends.
    chain.clone().extend(good).unwrap();

    // Root fabrications.
    let other_admin = admin();
    let forged_root = root_record(
        &other_admin,
        CLUSTER,
        DOMAIN,
        &manifest(CLUSTER, DOMAIN, &[w.n(1), w.n(2), w.n(3)]),
        &[w.n(1), w.n(2), w.n(3)],
    );
    assert_eq!(
        ConfigurationChain::from_genesis(&w.anchor, forged_root).err(),
        Some(ChainError::GenesisSignature)
    );
    let other_voters = root_record(
        &w.admin,
        CLUSTER,
        DOMAIN,
        &manifest(CLUSTER, DOMAIN, &[w.n(1), w.n(2), w.n(3)]),
        &[w.n(1), w.n(2), w.n(4)],
    );
    assert_eq!(
        ConfigurationChain::from_genesis(&w.anchor, other_voters).err(),
        Some(ChainError::GenesisVotersMismatch)
    );
    let other_manifest = root_record(
        &w.admin,
        CLUSTER,
        DOMAIN,
        &manifest(CLUSTER, DOMAIN, &[w.n(1), w.n(2)]),
        &[w.n(1), w.n(2), w.n(3)],
    );
    assert_eq!(
        ConfigurationChain::from_genesis(&w.anchor, other_manifest).err(),
        Some(ChainError::GenesisManifestMismatch)
    );
    // Handoff evidence at the root (internally consistent: old epoch zero).
    let mut handoff_root = handoff_record(&w.root, &[w.n(1), w.n(2), w.n(3)], &[w.n(1), w.n(2)]);
    handoff_root.epoch = epoch(1);
    if let ActivationEvidenceV1::Handoff { old_epoch, .. } = &mut handoff_root.activation {
        *old_epoch = ConfigurationEpoch::ZERO;
    }
    assert_eq!(
        ConfigurationChain::from_genesis(&w.anchor, handoff_root).err(),
        Some(ChainError::EvidenceKind)
    );
}

fn resign_handoff(mut record: GroupConfigurationV1, approvers: &[&Node]) -> GroupConfigurationV1 {
    let message = record.activation_message();
    if let ActivationEvidenceV1::Handoff { approvals, .. } = &mut record.activation {
        *approvals = approvers.iter().map(|a| a.sign(&message)).collect();
    }
    record
}

#[test]
fn directory_is_not_transition_authority() {
    let w = World::new();
    let mut client = ClientConfiguration::bootstrap(&w.anchor, w.root.clone()).unwrap();
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    // A directory (or controller) that merely asserts a new epoch.
    let mut asserted = handoff_record(&w.root, &five, &[]);
    if let ActivationEvidenceV1::Handoff { approvals, .. } = &mut asserted.activation {
        approvals.clear();
    }
    let response = BootstrapResponseV1 {
        records: vec![asserted],
        complete: true,
        ballot: None,
        endpoints: None,
    };
    assert_eq!(
        client.apply_bootstrap(&response),
        Err(ChainError::InsufficientApprovals { have: 0, need: 2 })
    );
    assert_eq!(client.current().epoch(), epoch(1));
    // The same directory carrying real old-quorum approvals: the evidence,
    // not the directory, installs it.
    let real = handoff_record(&w.root, &five, &[w.n(2), w.n(3)]);
    let response = BootstrapResponseV1 {
        records: vec![w.root.clone(), real.clone()],
        complete: true,
        ballot: Some(ballot_record(
            &real,
            1,
            w.n(1),
            &[w.n(1), w.n(2), w.n(3)],
            &[w.n(1), w.n(4), w.n(5)],
        )),
        endpoints: Some(endpoint_catalog(
            2,
            1,
            &[(w.n(1), inc(1)), (w.n(4), inc(1))],
            w.n(2),
        )),
    };
    let outcome = client.apply_bootstrap(&response).unwrap();
    assert_eq!(outcome.advanced, 1);
    assert_eq!(outcome.ballot, Some(Ok(())));
    assert_eq!(outcome.endpoints, Some(Ok(Installed::Advanced)));
    assert_eq!(client.current().epoch(), epoch(2));
    // Monotonic: stale, held and divergent records.
    assert_eq!(
        client.install_configuration(w.root.clone()),
        Err(ChainError::Stale)
    );
    assert_eq!(
        client.install_configuration(real.clone()),
        Ok(Installed::AlreadyHeld)
    );
    let divergent = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    assert_eq!(
        client.install_configuration(divergent.clone()),
        Err(ChainError::Divergent)
    );
    assert_eq!(client.current().certificate(), real.certificate_hash());
    // A node whose chain holds the other epoch-two record answers a
    // bootstrap naming the client's known epoch and certificate with that
    // record, so the client detects the divergence instead of applying an
    // empty answer and believing itself current.
    let mut other = w.chain();
    other.extend(divergent.clone()).unwrap();
    let (records, complete) =
        other.records_after(Some(epoch(2)), Some(real.certificate_hash()), 64);
    assert_eq!(records, vec![divergent]);
    assert!(complete);
    let response = BootstrapResponseV1 {
        records,
        complete,
        ballot: None,
        endpoints: None,
    };
    assert_eq!(
        client.apply_bootstrap(&response),
        Err(ChainError::Divergent)
    );
    assert_eq!(client.current().certificate(), real.certificate_hash());
}

#[test]
fn ballot_fast_set_is_fixed_and_only_voters_promise() {
    let w = World::new();
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    let e2 = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    // Built but never installed: a ballot may name an epoch the chain has
    // not reached.
    let e3 = handoff_record(&e2, &five, &[w.n(1), w.n(2), w.n(3)]);
    let mut client = ClientConfiguration::bootstrap(&w.anchor, w.root.clone()).unwrap();
    client.install_configuration(e2.clone()).unwrap();
    let leader = w.n(1);
    let promisers = [w.n(1), w.n(2), w.n(3)];

    let b1 = ballot_record(&e2, 1, leader, &[w.n(1), w.n(2), w.n(3)], &promisers);
    client.install_ballot(b1.clone()).unwrap();
    let held = client.ballot().unwrap();
    assert_eq!(held.fast_size(), 3);
    assert!(held.fast_eligible(&node(2)));
    assert!(!held.fast_eligible(&node(4)));
    // An arbitrary "fastest majority" for another request under the same
    // ballot is refused: the fast set is immutable within the ballot.
    let other = ballot_record(&e2, 1, leader, &[w.n(1), w.n(4), w.n(5)], &promisers);
    assert_eq!(
        client.install_ballot(other),
        Err(BallotError::FastSetChanged)
    );
    assert!(client.ballot().unwrap().fast_eligible(&node(2)));
    // Re-installing the same ballot is idempotent.
    client.install_ballot(b1).unwrap();
    // A higher ballot may choose a new fast set (with fresh promises).
    let b2 = ballot_record(
        &e2,
        2,
        w.n(4),
        &[w.n(4), w.n(5), w.n(1)],
        &[w.n(4), w.n(5), w.n(2)],
    );
    client.install_ballot(b2).unwrap();
    assert_eq!(client.ballot().unwrap().ballot().number, 2);
    // A lower ballot is stale.
    let b0 = ballot_record(&e2, 1, leader, &[w.n(1), w.n(2), w.n(3)], &promisers);
    assert_eq!(client.install_ballot(b0), Err(BallotError::Stale));

    // Source quorum rules: not a majority, excludes the leader, non-voter.
    assert_eq!(
        client.install_ballot(ballot_record(&e2, 3, leader, &[w.n(1), w.n(2)], &promisers)),
        Err(BallotError::Quorum(ConfigurationError::FastSetNotMajority))
    );
    assert_eq!(
        client.install_ballot(ballot_record(
            &e2,
            3,
            leader,
            &[w.n(2), w.n(3), w.n(4)],
            &promisers
        )),
        Err(BallotError::Quorum(
            ConfigurationError::FastSetExcludesLeader
        ))
    );
    assert_eq!(
        client.install_ballot(ballot_record(
            &e2,
            3,
            leader,
            &[w.n(1), w.n(2), w.n(6)],
            &promisers
        )),
        Err(BallotError::Quorum(ConfigurationError::FastSetNotVoters))
    );
    // Evidence rules: an observer's promise, a minority, a stale key.
    assert_eq!(
        client.install_ballot(ballot_record(
            &e2,
            3,
            leader,
            &[w.n(1), w.n(2), w.n(3)],
            &[w.n(1), w.n(2), w.n(6)]
        )),
        Err(BallotError::Evidence(EvidenceError::NotVoter {
            node: node(6)
        }))
    );
    assert_eq!(
        client.install_ballot(ballot_record(
            &e2,
            3,
            leader,
            &[w.n(1), w.n(2), w.n(3)],
            &[w.n(1), w.n(2)]
        )),
        Err(BallotError::Evidence(EvidenceError::Insufficient {
            have: 2,
            need: 3
        }))
    );
    let one_v2 = Node::new(1, 2);
    assert_eq!(
        client.install_ballot(ballot_record(
            &e2,
            3,
            leader,
            &[w.n(1), w.n(2), w.n(3)],
            &[&one_v2, w.n(2), w.n(3)]
        )),
        Err(BallotError::Evidence(EvidenceError::WrongIncarnation {
            node: node(1)
        }))
    );
    // Another epoch's ballot: stale or unknown.
    assert_eq!(
        client.install_ballot(ballot_record(
            &w.root,
            9,
            leader,
            &[w.n(1), w.n(2)],
            &[w.n(1), w.n(2)]
        )),
        Err(BallotError::Stale)
    );
    assert_eq!(
        client.install_ballot(ballot_record(
            &e3,
            1,
            leader,
            &[w.n(1), w.n(2), w.n(3)],
            &promisers
        )),
        Err(BallotError::Evidence(EvidenceError::UnknownEpoch))
    );
    // The held epoch under another configuration record of that epoch: the
    // promisers named a certificate this chain does not hold, so whatever
    // they promised, they did not promise it here.
    assert_eq!(
        client.install_ballot(ballot_for(
            CLUSTER,
            DOMAIN,
            epoch(2),
            Digest32([0x11; 32]),
            3,
            leader,
            &[w.n(1), w.n(2), w.n(3)],
            &promisers
        )),
        Err(BallotError::CertificateMismatch)
    );
}

#[test]
fn a_ballot_certificate_of_one_domain_is_refused_by_another_that_shares_its_voters() {
    let w = World::new();
    // Two domains of one deployment: the same voters at the same
    // incarnations under the same keys, the same admin, the same quorum
    // policy and the same epoch numbering. Only the domain differs, so
    // nothing but what the promises actually say can tell the two apart.
    let sibling = w.sibling(CLUSTER, DomainId([3; 16]));
    let mut here = ClientConfiguration::bootstrap(&w.anchor, w.root.clone()).unwrap();
    let mut there = ClientConfiguration::bootstrap(&sibling.anchor, sibling.root.clone()).unwrap();
    assert_eq!(here.current().epoch(), there.current().epoch());
    assert_eq!(here.current().voters(), there.current().voters());

    let promisers = [w.n(1), w.n(2)];
    let mine = ballot_record(&w.root, 7, w.n(1), &[w.n(1), w.n(2)], &promisers);
    here.install_ballot(mine.clone()).unwrap();
    // Valid evidence, replayed. Every ballot and epoch number matches what
    // the receiving domain would accept, the signers are its own voters at
    // its own incarnations, and the signatures verify under the keys it
    // holds. It is still a promise about somewhere else.
    assert_eq!(
        there.install_ballot(mine.clone()),
        Err(BallotError::OriginMismatch)
    );
    assert!(there.ballot().is_none());
    // Relabelling the certificate with the receiving domain does not
    // launder it: the configuration it names is not the one held there.
    let relabelled = BallotConfigurationV1 {
        domain: sibling.root.domain,
        ..mine.clone()
    };
    assert_eq!(
        there.install_ballot(relabelled),
        Err(BallotError::CertificateMismatch)
    );
    // Restamping both is what the binding costs an attacker: the promises
    // are over the old context, so they no longer verify at all.
    let restamped = BallotConfigurationV1 {
        domain: sibling.root.domain,
        configuration_certificate: sibling.root.certificate_hash(),
        ..mine.clone()
    };
    assert_eq!(
        there.install_ballot(restamped),
        Err(BallotError::Evidence(EvidenceError::Signature {
            node: node(1)
        }))
    );
    // The same holds across clusters that share a domain identifier.
    let restored = w.sibling(ClusterId([4; 16]), DOMAIN);
    let mut elsewhere =
        ClientConfiguration::bootstrap(&restored.anchor, restored.root.clone()).unwrap();
    assert_eq!(
        elsewhere.install_ballot(mine.clone()),
        Err(BallotError::OriginMismatch)
    );
    // The domain the promises were made in still holds them.
    assert_eq!(here.ballot().unwrap().ballot().number, 7);
    here.install_ballot(mine).unwrap();
}

#[test]
fn address_and_certificate_refresh_cannot_change_voters() {
    let w = World::new();
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    let e2 = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    let mut client = ClientConfiguration::bootstrap(&w.anchor, w.root.clone()).unwrap();
    client.install_configuration(e2).unwrap();
    let voters_before = client.current().voters();

    let c1 = endpoint_catalog(2, 1, &[(w.n(1), inc(1)), (w.n(2), inc(1))], w.n(3));
    assert_eq!(
        client.install_endpoints(c1.clone()),
        Ok(Installed::Advanced)
    );
    // New addresses and certificate fingerprints under a higher generation.
    let c2 = endpoint_catalog(
        2,
        2,
        &[(w.n(1), inc(1)), (w.n(2), inc(1)), (w.n(3), inc(1))],
        w.n(1),
    );
    assert_eq!(
        client.install_endpoints(c2.clone()),
        Ok(Installed::Advanced)
    );
    assert_eq!(client.install_endpoints(c1), Err(CatalogError::Stale));
    assert_eq!(client.install_endpoints(c2), Ok(Installed::AlreadyHeld));
    // A catalog listing a stranger, or a voter under a new incarnation.
    assert_eq!(
        client.install_endpoints(endpoint_catalog(2, 3, &[(w.n(6), inc(1))], w.n(1))),
        Err(CatalogError::NotVoter { node: node(6) })
    );
    assert_eq!(
        client.install_endpoints(endpoint_catalog(2, 3, &[(w.n(1), inc(2))], w.n(1))),
        Err(CatalogError::WrongIncarnation { node: node(1) })
    );
    // Attested by a non-voter.
    assert_eq!(
        client.install_endpoints(endpoint_catalog(2, 3, &[(w.n(1), inc(1))], w.n(6))),
        Err(CatalogError::Evidence(EvidenceError::NotVoter {
            node: node(6)
        }))
    );
    // A catalog for an epoch the chain does not hold installs nothing.
    assert_eq!(
        client.install_endpoints(endpoint_catalog(3, 1, &[(w.n(1), inc(1))], w.n(1))),
        Err(CatalogError::UnknownEpoch)
    );
    // Whatever the catalogs said, the voter set is the chain's.
    assert_eq!(client.current().voters(), voters_before);
    assert_eq!(client.endpoints().unwrap().generation.get(), 2);

    // Observers: serving topology only; a voter is never an observer.
    let o1 = observer_catalog(2, 1, &[w.n(6), w.n(7)], w.n(2));
    assert_eq!(client.install_observers(o1), Ok(Installed::Advanced));
    assert_eq!(
        client.install_observers(observer_catalog(2, 2, &[w.n(6), w.n(3)], w.n(2))),
        Err(CatalogError::ObserverIsVoter { node: node(3) })
    );
    assert_eq!(
        client.install_observers(observer_catalog(2, 1, &[w.n(6)], w.n(2))),
        Err(CatalogError::Stale)
    );
    assert_eq!(client.current().voters(), voters_before);
    assert_eq!(client.observers().unwrap().observers.len(), 2);
}

#[test]
fn hints_only_trigger_refreshes() {
    let w = World::new();
    let mut client = ClientConfiguration::bootstrap(&w.anchor, w.root.clone()).unwrap();
    let hint = |epoch_no: u64, certificate: Digest32, eg: u64, cg: u64| ConfigurationHintV1 {
        cluster: CLUSTER,
        domain: DOMAIN,
        epoch: epoch(epoch_no),
        certificate,
        endpoint_generation: EndpointGeneration::new(eg).unwrap(),
        catalog_generation: CatalogGeneration::new(cg).unwrap(),
    };
    let cert = client.current().certificate();
    assert_eq!(
        client.observe_hint(&hint(5, Digest32([0; 32]), 0, 0)),
        HintDecision::RefreshConfiguration { epoch: epoch(5) }
    );
    assert_eq!(
        client.current().epoch(),
        epoch(1),
        "a hint installs nothing"
    );
    let mut foreign = hint(5, Digest32([0; 32]), 0, 0);
    foreign.cluster = ClusterId([9; 16]);
    assert_eq!(client.observe_hint(&foreign), HintDecision::Foreign);
    assert_eq!(
        client.observe_hint(&hint(1, Digest32([0; 32]), 0, 0)),
        HintDecision::Foreign,
        "same epoch, other certificate: a diverged sender"
    );
    assert_eq!(
        client.observe_hint(&hint(1, cert, 1, 0)),
        HintDecision::RefreshEndpoints
    );
    client
        .install_endpoints(endpoint_catalog(1, 1, &[(w.n(1), inc(1))], w.n(2)))
        .unwrap();
    assert_eq!(
        client.observe_hint(&hint(1, cert, 1, 1)),
        HintDecision::RefreshObservers
    );
    client
        .install_observers(observer_catalog(1, 1, &[w.n(6)], w.n(2)))
        .unwrap();
    assert_eq!(
        client.observe_hint(&hint(1, cert, 1, 1)),
        HintDecision::UpToDate
    );
    assert_eq!(
        client.observe_hint(&hint(1, cert, 2, 1)),
        HintDecision::RefreshEndpoints
    );
}

#[test]
fn historical_evidence_verifies_without_an_issuer() {
    let w = World::new();
    let mut chain = w.chain();
    let five = [w.n(1), w.n(2), w.n(3), w.n(4), w.n(5)];
    let e2 = handoff_record(&w.root, &five, &[w.n(1), w.n(2)]);
    chain.extend(e2.clone()).unwrap();
    let one_v2 = Node::new(1, 2);
    let e3 = handoff_record(&e2, &[&one_v2, w.n(6), w.n(7)], &[w.n(2), w.n(3), w.n(4)]);
    chain.extend(e3).unwrap();

    // A delayed result signed by node 2 at epoch 1 (node 2 is no longer a
    // voter, and node 1's key rotated since) still verifies against the
    // epoch-1 record; the same signature is not epoch-3 evidence.
    let message = Digest32([0x42; 32]);
    let old = w.n(2).sign(&message);
    assert_eq!(
        chain.verify_voter_signature(epoch(1), &old, &message),
        Ok(())
    );
    assert_eq!(
        chain.verify_voter_signature(epoch(2), &old, &message),
        Ok(())
    );
    assert_eq!(
        chain.verify_voter_signature(epoch(3), &old, &message),
        Err(EvidenceError::NotVoter { node: node(2) })
    );
    let old_one = w.n(1).sign(&message);
    assert_eq!(
        chain.verify_voter_signature(epoch(1), &old_one, &message),
        Ok(())
    );
    assert_eq!(
        chain.verify_voter_signature(epoch(3), &old_one, &message),
        Err(EvidenceError::WrongIncarnation { node: node(1) })
    );
    assert_eq!(
        chain.verify_voter_signature(epoch(7), &old_one, &message),
        Err(EvidenceError::UnknownEpoch)
    );
    // An old epoch's ballot configuration verifies as history too.
    let b = ballot_record(&w.root, 4, w.n(1), &[w.n(1), w.n(2)], &[w.n(1), w.n(3)]);
    let config = chain.verify_ballot(&b).unwrap();
    assert_eq!(config.epoch(), epoch(1));
    assert_eq!(config.fast_size(), 2);
}
