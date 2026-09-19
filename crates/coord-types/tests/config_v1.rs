//! Shape validation, configuration frames, observer paging and the frozen
//! record/frame fixture.

use std::path::PathBuf;

use coord_types::config_v1::{
    ActivationEvidenceV1, BallotConfigurationV1, BootstrapRequestV1, BootstrapResponseV1,
    ConfigError, ConfigurationHintV1, EndpointCatalogV1, EndpointV1, GroupConfigurationV1,
    NO_PREVIOUS_CERTIFICATE, NoticeV1, ObserverCatalogV1, ObserverDiscoveryPageV1,
    ObserverDiscoveryRequestV1, ObserverEntryV1, QuorumPolicyId, SubscribeV1, VERSION,
    VoterRecordV1, VoterSignatureV1, decode_message, encode_message, kinds, limits,
};
use coord_types::identity::Digest32;
use coord_types::ids::{
    Ballot, CatalogGeneration, ClusterId, ConfigurationEpoch, DomainId, EndpointGeneration,
    ReplicaId, ReplicaIncarnation,
};
use coord_types::wire_v1::{FrameReader, WireError};
use serde::{Deserialize, Serialize};

fn node(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn inc(n: u64) -> ReplicaIncarnation {
    ReplicaIncarnation::new(n).unwrap()
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn key(n: u8) -> Vec<u8> {
    let mut k = vec![0x04u8];
    k.extend(std::iter::repeat_n(n, 64));
    k
}

fn voter(n: u8) -> VoterRecordV1 {
    VoterRecordV1 {
        node: node(n),
        incarnation: inc(1),
        public_key: key(n),
    }
}

fn sig(n: u8) -> VoterSignatureV1 {
    VoterSignatureV1 {
        node: node(n),
        incarnation: inc(1),
        signature: vec![n; 64],
    }
}

fn genesis_record() -> GroupConfigurationV1 {
    GroupConfigurationV1 {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(1),
        voters: vec![voter(1), voter(2), voter(3)],
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        previous_certificate: NO_PREVIOUS_CERTIFICATE,
        activation: ActivationEvidenceV1::Genesis {
            manifest_digest: Digest32([0xaa; 32]),
            admin_signature: vec![7; 64],
        },
    }
}

fn handoff_record(previous: Digest32) -> GroupConfigurationV1 {
    GroupConfigurationV1 {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(2),
        voters: vec![voter(1), voter(2), voter(3), voter(4), voter(5)],
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        previous_certificate: previous,
        activation: ActivationEvidenceV1::Handoff {
            old_epoch: epoch(1),
            terminal_certificate: Digest32([0xbb; 32]),
            approvals: vec![sig(1), sig(2)],
        },
    }
}

fn ballot_record() -> BallotConfigurationV1 {
    BallotConfigurationV1 {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(2),
        configuration_certificate: Digest32([0xdd; 32]),
        ballot: Ballot {
            epoch: epoch(2),
            number: 3,
            leader: node(1),
        },
        quorum_policy: QuorumPolicyId::C2_FIXED_MAJORITY,
        fast_set: vec![node(1), node(2), node(3)],
        promises: vec![sig(1), sig(2), sig(3)],
    }
}

fn endpoints() -> EndpointCatalogV1 {
    EndpointCatalogV1 {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(2),
        generation: EndpointGeneration::new(4).unwrap(),
        endpoints: vec![EndpointV1 {
            node: node(1),
            incarnation: inc(1),
            addresses: vec!["voter-1.example:7443".to_owned()],
            certificate_fingerprint: Some(Digest32([0xcc; 32])),
        }],
        attestation: sig(2),
    }
}

fn observer(n: u8) -> ObserverEntryV1 {
    ObserverEntryV1 {
        node: node(n),
        incarnation: inc(1),
        region: "eu-west".to_owned(),
        addresses: vec![format!("observer-{n}.example:7443")],
        capabilities: 0b11,
    }
}

fn observers() -> ObserverCatalogV1 {
    ObserverCatalogV1 {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(2),
        generation: CatalogGeneration::new(9).unwrap(),
        observers: (10..15).map(observer).collect(),
        attestation: sig(3),
    }
}

#[test]
fn record_shape_is_validated() {
    let g = genesis_record();
    g.validate_shape().unwrap();
    assert!(g.is_genesis());
    assert_eq!(g.majority(), 2);
    let cases: Vec<(GroupConfigurationV1, ConfigError)> = vec![
        (
            GroupConfigurationV1 {
                voters: vec![],
                ..g.clone()
            },
            ConfigError::NoVoters,
        ),
        (
            GroupConfigurationV1 {
                voters: (0..17).map(voter).collect(),
                ..g.clone()
            },
            ConfigError::TooManyVoters,
        ),
        (
            GroupConfigurationV1 {
                voters: vec![voter(2), voter(1)],
                ..g.clone()
            },
            ConfigError::VotersNotSortedUnique,
        ),
        (
            GroupConfigurationV1 {
                voters: vec![voter(1), voter(1)],
                ..g.clone()
            },
            ConfigError::VotersNotSortedUnique,
        ),
        (
            GroupConfigurationV1 {
                voters: vec![VoterRecordV1 {
                    public_key: vec![0x02; 65],
                    ..voter(1)
                }],
                ..g.clone()
            },
            ConfigError::BadPublicKey,
        ),
        (
            GroupConfigurationV1 {
                voters: vec![VoterRecordV1 {
                    public_key: vec![0x04; 33],
                    ..voter(1)
                }],
                ..g.clone()
            },
            ConfigError::BadPublicKey,
        ),
        (
            GroupConfigurationV1 {
                quorum_policy: QuorumPolicyId(9),
                ..g.clone()
            },
            ConfigError::UnsupportedPolicy,
        ),
        (
            GroupConfigurationV1 {
                previous_certificate: Digest32([1; 32]),
                ..g.clone()
            },
            ConfigError::PreviousCertificateShape,
        ),
        (
            GroupConfigurationV1 {
                activation: ActivationEvidenceV1::Genesis {
                    manifest_digest: Digest32([0; 32]),
                    admin_signature: vec![1; 63],
                },
                ..g.clone()
            },
            ConfigError::BadSignature,
        ),
        (
            handoff_record(NO_PREVIOUS_CERTIFICATE),
            ConfigError::PreviousCertificateShape,
        ),
        (
            GroupConfigurationV1 {
                epoch: epoch(3),
                ..handoff_record(Digest32([1; 32]))
            },
            ConfigError::OldEpochNotPrevious,
        ),
        (
            GroupConfigurationV1 {
                activation: ActivationEvidenceV1::Handoff {
                    old_epoch: epoch(1),
                    terminal_certificate: Digest32([0; 32]),
                    approvals: vec![sig(1), sig(1)],
                },
                ..handoff_record(Digest32([1; 32]))
            },
            ConfigError::DuplicateSigner,
        ),
    ];
    for (record, expected) in cases {
        assert_eq!(record.validate_shape(), Err(expected));
    }
    // The activation message excludes signatures; the certificate hash
    // includes them.
    let h = handoff_record(g.certificate_hash());
    let mut resigned = h.clone();
    if let ActivationEvidenceV1::Handoff { approvals, .. } = &mut resigned.activation {
        approvals[0].signature = vec![9; 64];
    }
    assert_eq!(h.activation_message(), resigned.activation_message());
    assert_ne!(h.certificate_hash(), resigned.certificate_hash());
    let mut other_voters = h.clone();
    other_voters.voters.pop();
    assert_ne!(h.activation_message(), other_voters.activation_message());
    assert_ne!(h.activation_message(), h.certificate_hash());
}

#[test]
fn ballot_and_catalog_shapes_are_validated() {
    let b = ballot_record();
    b.validate_shape().unwrap();
    let mut wrong_epoch = b.clone();
    wrong_epoch.ballot.epoch = epoch(1);
    assert_eq!(
        wrong_epoch.validate_shape(),
        Err(ConfigError::EpochMismatch)
    );
    assert_eq!(
        BallotConfigurationV1 {
            fast_set: vec![node(2), node(1)],
            ..b.clone()
        }
        .validate_shape(),
        Err(ConfigError::BadFastSet)
    );
    assert_eq!(
        BallotConfigurationV1 {
            fast_set: vec![],
            ..b.clone()
        }
        .validate_shape(),
        Err(ConfigError::BadFastSet)
    );
    assert_eq!(
        BallotConfigurationV1 {
            promises: vec![sig(1), sig(1)],
            ..b.clone()
        }
        .validate_shape(),
        Err(ConfigError::DuplicateSigner)
    );
    let mut other_fast = b.clone();
    other_fast.fast_set = vec![node(1), node(2), node(4)];
    assert_ne!(b.ballot_message(), other_fast.ballot_message());
    // The context a promise was made in is part of what it signs: the same
    // ballot under another domain, cluster or configuration record is a
    // different message, so its promises cannot be carried there.
    let mut other_domain = b.clone();
    other_domain.domain = DomainId([3; 16]);
    assert_ne!(b.ballot_message(), other_domain.ballot_message());
    let mut other_cluster = b.clone();
    other_cluster.cluster = ClusterId([9; 16]);
    assert_ne!(b.ballot_message(), other_cluster.ballot_message());
    let mut other_certificate = b.clone();
    other_certificate.configuration_certificate = Digest32([0xde; 32]);
    assert_ne!(b.ballot_message(), other_certificate.ballot_message());

    let e = endpoints();
    e.validate_shape().unwrap();
    let mut long = e.clone();
    long.endpoints[0].addresses = vec!["x".repeat(limits::MAX_ADDRESS_BYTES + 1)];
    assert_eq!(long.validate_shape(), Err(ConfigError::TooLong));
    let mut many = e.clone();
    many.endpoints[0].addresses = vec!["a".to_owned(); limits::MAX_ADDRESSES + 1];
    assert_eq!(many.validate_shape(), Err(ConfigError::TooMany));
    let mut unsorted = e.clone();
    unsorted.endpoints.push(EndpointV1 {
        node: node(0),
        ..e.endpoints[0].clone()
    });
    assert_eq!(
        unsorted.validate_shape(),
        Err(ConfigError::VotersNotSortedUnique)
    );
    let mut regen = e.clone();
    regen.generation = EndpointGeneration::new(5).unwrap();
    assert_ne!(e.catalog_message(), regen.catalog_message());

    let o = observers();
    o.validate_shape().unwrap();
    let mut region = o.clone();
    region.observers[0].region = "r".repeat(limits::MAX_REGION_BYTES + 1);
    assert_eq!(region.validate_shape(), Err(ConfigError::TooLong));
    // Endpoint and observer catalog messages never collide.
    assert_ne!(e.catalog_message(), o.catalog_message());
}

#[test]
fn observer_paging_is_deterministic_and_complete() {
    let o = observers();
    let p1 = o.page(None, 2);
    p1.validate_shape().unwrap();
    assert_eq!(p1.entries.len(), 2);
    assert_eq!(p1.next, Some(node(11)));
    assert!(!p1.complete);
    let p2 = o.page(p1.next, 2);
    assert_eq!(p2.entries[0].node, node(12));
    assert_eq!(p2.next, Some(node(13)));
    let p3 = o.page(p2.next, 2);
    assert_eq!(p3.entries.len(), 1);
    assert!(p3.complete);
    assert_eq!(p3.next, None);
    let all = o.page(None, limits::MAX_PAGE);
    assert_eq!(all.entries.len(), 5);
    assert!(all.complete);
    // A zero or oversized limit is clamped, never a panic or an empty
    // incomplete page.
    assert_eq!(o.page(None, 0).entries.len(), 1);
    assert_eq!(o.page(None, u16::MAX).entries.len(), 5);
    assert!(o.page(Some(node(14)), 2).complete);
    assert_eq!(
        ObserverDiscoveryRequestV1 {
            cluster: o.cluster,
            domain: o.domain,
            after: None,
            limit: 0,
        }
        .validate_shape(),
        Err(ConfigError::BadLimit)
    );
    let mut inconsistent = p1.clone();
    inconsistent.complete = true;
    assert_eq!(inconsistent.validate_shape(), Err(ConfigError::BadLimit));
}

#[test]
fn configuration_frames_round_trip_exactly() {
    let g = genesis_record();
    let request = BootstrapRequestV1 {
        cluster: g.cluster,
        domain: g.domain,
        known_epoch: Some(epoch(1)),
        known_certificate: Some(g.certificate_hash()),
    };
    let response = BootstrapResponseV1 {
        records: vec![g.clone(), handoff_record(g.certificate_hash())],
        complete: true,
        ballot: Some(ballot_record()),
        endpoints: Some(endpoints()),
    };
    response.validate_shape().unwrap();
    let subscribe = SubscribeV1 {
        cluster: g.cluster,
        domain: g.domain,
        epoch: epoch(2),
        endpoint_generation: EndpointGeneration::new(4).unwrap(),
        catalog_generation: CatalogGeneration::new(9).unwrap(),
    };
    let notice = NoticeV1 {
        hint: ConfigurationHintV1 {
            cluster: g.cluster,
            domain: g.domain,
            epoch: epoch(2),
            certificate: Digest32([3; 32]),
            endpoint_generation: EndpointGeneration::new(5).unwrap(),
            catalog_generation: CatalogGeneration::new(9).unwrap(),
        },
    };
    let discovery = ObserverDiscoveryRequestV1 {
        cluster: g.cluster,
        domain: g.domain,
        after: Some(node(11)),
        limit: 2,
    };
    let page = observers().page(Some(node(11)), 2);

    let frames = [
        encode_message(kinds::BOOTSTRAP_REQUEST, &request).unwrap(),
        encode_message(kinds::BOOTSTRAP_RESPONSE, &response).unwrap(),
        encode_message(kinds::SUBSCRIBE, &subscribe).unwrap(),
        encode_message(kinds::NOTICE, &notice).unwrap(),
        encode_message(kinds::OBSERVER_DISCOVERY_REQUEST, &discovery).unwrap(),
        encode_message(kinds::OBSERVER_DISCOVERY_PAGE, &page).unwrap(),
    ];
    let mut reader = FrameReader::new();
    for f in &frames {
        reader.push(f).expect("within the reader bound");
    }
    let decoded: Vec<_> = std::iter::from_fn(|| reader.next_frame().unwrap()).collect();
    reader.finish().unwrap();
    assert_eq!(decoded.len(), 6);
    assert!(decoded.iter().all(|f| f.version == VERSION));
    assert_eq!(
        decode_message::<BootstrapRequestV1>(&decoded[0], kinds::BOOTSTRAP_REQUEST).unwrap(),
        request
    );
    assert_eq!(
        decode_message::<BootstrapResponseV1>(&decoded[1], kinds::BOOTSTRAP_RESPONSE).unwrap(),
        response
    );
    assert_eq!(
        decode_message::<SubscribeV1>(&decoded[2], kinds::SUBSCRIBE).unwrap(),
        subscribe
    );
    assert_eq!(
        decode_message::<NoticeV1>(&decoded[3], kinds::NOTICE).unwrap(),
        notice
    );
    assert_eq!(
        decode_message::<ObserverDiscoveryRequestV1>(
            &decoded[4],
            kinds::OBSERVER_DISCOVERY_REQUEST
        )
        .unwrap(),
        discovery
    );
    assert_eq!(
        decode_message::<ObserverDiscoveryPageV1>(&decoded[5], kinds::OBSERVER_DISCOVERY_PAGE)
            .unwrap(),
        page
    );
    // Wrong kind, wrong version and trailing bytes are rejected.
    assert!(matches!(
        decode_message::<SubscribeV1>(&decoded[0], kinds::SUBSCRIBE),
        Err(ConfigError::Wire(WireError::UnsupportedKind { .. }))
    ));
    let mut v2 = decoded[2].clone();
    v2.version = 2;
    assert!(decode_message::<SubscribeV1>(&v2, kinds::SUBSCRIBE).is_err());
    let mut trailing = decoded[2].clone();
    trailing.payload.push(0);
    assert!(matches!(
        decode_message::<SubscribeV1>(&trailing, kinds::SUBSCRIBE),
        Err(ConfigError::Wire(WireError::TrailingPayloadBytes {
            extra: 1
        }))
    ));
    // The largest well-formed response (64 records of 16 voters with 16
    // approvals each, plus a full endpoint catalog) fits the configuration
    // class limit (256 KiB); anything above the bound is refused before
    // framing, and a raw oversized payload by the frame encoder.
    let mut big = g.clone();
    big.voters = (1..=16).map(voter).collect();
    big.activation = ActivationEvidenceV1::Handoff {
        old_epoch: epoch(1),
        terminal_certificate: Digest32([0; 32]),
        approvals: (1..=16).map(sig).collect(),
    };
    big.previous_certificate = Digest32([1; 32]);
    big.epoch = epoch(2);
    big.validate_shape().unwrap();
    let mut full_endpoints = endpoints();
    full_endpoints.endpoints = (1..=16)
        .map(|n| EndpointV1 {
            node: node(n),
            incarnation: inc(1),
            addresses: vec!["a".repeat(limits::MAX_ADDRESS_BYTES); limits::MAX_ADDRESSES],
            certificate_fingerprint: Some(Digest32([n; 32])),
        })
        .collect();
    let maximal = BootstrapResponseV1 {
        records: vec![big.clone(); limits::MAX_CHAIN_RECORDS],
        complete: true,
        ballot: Some(ballot_record()),
        endpoints: Some(full_endpoints),
    };
    maximal.validate_shape().unwrap();
    let frame = encode_message(kinds::BOOTSTRAP_RESPONSE, &maximal).unwrap();
    assert!(frame.len() <= 256 * 1024 + 4);
    let over = BootstrapResponseV1 {
        records: vec![big; limits::MAX_CHAIN_RECORDS + 1],
        ..maximal
    };
    assert_eq!(over.validate_shape(), Err(ConfigError::TooMany));
    assert!(matches!(
        encode_message(kinds::BOOTSTRAP_RESPONSE, &vec![0u8; 300 * 1024]),
        Err(ConfigError::Wire(WireError::PayloadTooLarge))
    ));
    // Kinds sit in the reserved configuration range.
    for k in [
        kinds::BOOTSTRAP_REQUEST,
        kinds::BOOTSTRAP_RESPONSE,
        kinds::SUBSCRIBE,
        kinds::NOTICE,
        kinds::OBSERVER_DISCOVERY_REQUEST,
        kinds::OBSERVER_DISCOVERY_PAGE,
    ] {
        assert_eq!(k >> 8, 0x04);
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ConfigFixture {
    schema: String,
    version: u16,
    kinds: Vec<(String, u16)>,
    activation_context: String,
    record_context: String,
    ballot_context: String,
    catalog_context: String,
    genesis_activation_message_hex: String,
    genesis_certificate_hex: String,
    handoff_activation_message_hex: String,
    handoff_certificate_hex: String,
    ballot_message_hex: String,
    endpoint_catalog_message_hex: String,
    observer_catalog_message_hex: String,
    frames_hex: Vec<(String, String)>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn configuration_fixture_is_frozen() {
    let g = genesis_record();
    let h = handoff_record(g.certificate_hash());
    let request = BootstrapRequestV1 {
        cluster: g.cluster,
        domain: g.domain,
        known_epoch: None,
        known_certificate: None,
    };
    let response = BootstrapResponseV1 {
        records: vec![g.clone(), h.clone()],
        complete: true,
        ballot: Some(ballot_record()),
        endpoints: Some(endpoints()),
    };
    let subscribe = SubscribeV1 {
        cluster: g.cluster,
        domain: g.domain,
        epoch: epoch(2),
        endpoint_generation: EndpointGeneration::new(4).unwrap(),
        catalog_generation: CatalogGeneration::new(9).unwrap(),
    };
    let notice = NoticeV1 {
        hint: ConfigurationHintV1 {
            cluster: g.cluster,
            domain: g.domain,
            epoch: epoch(2),
            certificate: h.certificate_hash(),
            endpoint_generation: EndpointGeneration::new(4).unwrap(),
            catalog_generation: CatalogGeneration::new(9).unwrap(),
        },
    };
    let discovery = ObserverDiscoveryRequestV1 {
        cluster: g.cluster,
        domain: g.domain,
        after: None,
        limit: 2,
    };
    let page = observers().page(None, 2);
    let fixture = ConfigFixture {
        schema: "config_frames_v1".to_owned(),
        version: VERSION,
        kinds: vec![
            ("BootstrapRequest".to_owned(), kinds::BOOTSTRAP_REQUEST),
            ("BootstrapResponse".to_owned(), kinds::BOOTSTRAP_RESPONSE),
            ("Subscribe".to_owned(), kinds::SUBSCRIBE),
            ("Notice".to_owned(), kinds::NOTICE),
            (
                "ObserverDiscoveryRequest".to_owned(),
                kinds::OBSERVER_DISCOVERY_REQUEST,
            ),
            (
                "ObserverDiscoveryPage".to_owned(),
                kinds::OBSERVER_DISCOVERY_PAGE,
            ),
        ],
        activation_context: coord_types::HashDomain::ConfigurationActivation
            .context()
            .to_owned(),
        record_context: coord_types::HashDomain::ConfigurationRecord
            .context()
            .to_owned(),
        ballot_context: coord_types::HashDomain::ConfigurationBallot
            .context()
            .to_owned(),
        catalog_context: coord_types::HashDomain::ConfigurationCatalog
            .context()
            .to_owned(),
        genesis_activation_message_hex: hex(&g.activation_message().0),
        genesis_certificate_hex: hex(&g.certificate_hash().0),
        handoff_activation_message_hex: hex(&h.activation_message().0),
        handoff_certificate_hex: hex(&h.certificate_hash().0),
        ballot_message_hex: hex(&ballot_record().ballot_message().0),
        endpoint_catalog_message_hex: hex(&endpoints().catalog_message().0),
        observer_catalog_message_hex: hex(&observers().catalog_message().0),
        frames_hex: vec![
            (
                "bootstrap-request".to_owned(),
                hex(&encode_message(kinds::BOOTSTRAP_REQUEST, &request).unwrap()),
            ),
            (
                "bootstrap-response".to_owned(),
                hex(&encode_message(kinds::BOOTSTRAP_RESPONSE, &response).unwrap()),
            ),
            (
                "subscribe".to_owned(),
                hex(&encode_message(kinds::SUBSCRIBE, &subscribe).unwrap()),
            ),
            (
                "notice".to_owned(),
                hex(&encode_message(kinds::NOTICE, &notice).unwrap()),
            ),
            (
                "observer-discovery-request".to_owned(),
                hex(&encode_message(kinds::OBSERVER_DISCOVERY_REQUEST, &discovery).unwrap()),
            ),
            (
                "observer-discovery-page".to_owned(),
                hex(&encode_message(kinds::OBSERVER_DISCOVERY_PAGE, &page).unwrap()),
            ),
        ],
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/config_frames_v1.json");
    if std::env::var_os("COORD_TYPES_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: ConfigFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "configuration fixture drifted; records, digests and frames are frozen"
    );
}
