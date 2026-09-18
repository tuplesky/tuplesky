//! task-33 at packet level (over the quinn-proto simulation): the
//! frontend's fan-out reaches every voter over the frontend's own unary
//! lane at the same time, no voter relays the submission, and a native
//! client presenting the same frame is refused by the voter's ingress.

use std::sync::Arc;

use coord_collector::{IngressError, KIND_SUBMIT, SubmitV1, admitted_from_submit, submit_frame};
use coord_core::capability::{AdmissionReceipt, VerifierToken};
use coord_transport::{BoundIdentity, Lane, Limits};
use coord_transport_sim::{LinkFaults, NodeConfig, PacketWorld, SimEvent};
use coord_transport_testkit::{TestBinder, TestCa, TestIdentity};
use coord_types::RetryKey;
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::{Frame, PeerRole, RequestV1};

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

fn world(roles: &[PeerRole]) -> (PacketWorld, Vec<BoundIdentity>) {
    let ca = TestCa::new();
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    let mut ids: Vec<TestIdentity> = Vec::new();
    for (i, role) in roles.iter().enumerate() {
        let id = ca.issue(&format!("node-{i}"), r(i as u8), inc(), *role);
        binder.register(&id);
        ids.push(id);
    }
    let binder = Arc::new(binder);
    let expected: Vec<BoundIdentity> = ids.iter().map(TestIdentity::expected).collect();
    let configs = ids
        .into_iter()
        .map(|identity| NodeConfig {
            identity,
            roots: ca.roots(),
            cluster: CLUSTER,
            domain: DOMAIN,
            binder: binder.clone(),
            limits: Limits::default(),
            capabilities: vec![1],
        })
        .collect();
    (PacketWorld::new([9; 32], configs), expected)
}

fn symmetric() -> LinkFaults {
    LinkFaults {
        delay_min: 10,
        delay_max: 10,
        ..LinkFaults::default()
    }
}

fn submission() -> Vec<u8> {
    let mut logical = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    );
    logical.canonicalize();
    let key = RetryKey {
        cluster_id: CLUSTER,
        domain_id: DOMAIN,
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(1).unwrap(),
    };
    submit_frame(&SubmitV1 {
        receipt: coord_collector::AdmissionClaimsV1::of(&AdmissionReceipt::from_verifier(
            VerifierToken::for_boundary(),
            SessionId([3; 16]),
            1,
            u32::MAX,
            Digest32([6; 32]),
            0,
        )),
        request: RequestV1::new(key, &logical, 0).unwrap(),
    })
    .unwrap()
}

#[test]
fn the_fan_out_reaches_every_voter_at_once_and_nobody_relays_it() {
    let roles = [
        PeerRole::Voter,
        PeerRole::Voter,
        PeerRole::Voter,
        PeerRole::Frontend,
        PeerRole::Client,
    ];
    let (mut w, expected) = world(&roles);
    // Symmetric, jitter-free links: every path is the same delay.
    w.set_default_faults(symmetric());
    for (a, b) in [(0, 1), (1, 2), (2, 0)] {
        w.connect(
            a,
            b,
            PeerRole::Voter,
            Some(inc()),
            Lane::Control,
            &format!("node-{b}"),
            expected[b].clone(),
        )
        .unwrap();
    }
    for (v, exp) in expected.iter().enumerate().take(3) {
        w.connect(
            3,
            v,
            PeerRole::Frontend,
            None,
            Lane::Unary,
            &format!("node-{v}"),
            exp.clone(),
        )
        .unwrap();
    }
    w.connect(
        4,
        0,
        PeerRole::Client,
        None,
        Lane::Unary,
        "node-0",
        expected[0].clone(),
    )
    .unwrap();
    assert!(w.run_until(30_000, |w| {
        (0..5).map(|n| w.connections(n)).sum::<usize>() == 2 * 7
    }));
    for n in 0..5 {
        let _ = w.take_events(n);
    }

    // The fan-out: one frame to each voter, queued in the same tick.
    let frame = submission();
    let sent_at = w.ticks();
    for v in 0..3u8 {
        w.send(3, r(v), inc(), Lane::Unary, DOMAIN, frame.clone())
            .unwrap();
    }
    // The client presents the same frame to voter 0.
    w.send(4, r(0), inc(), Lane::Unary, DOMAIN, frame.clone())
        .unwrap();

    let mut arrivals: Vec<(usize, u64, Frame, PeerRole)> = Vec::new();
    let mut steps = 0;
    while arrivals.len() < 4 && steps < 50_000 {
        w.step();
        steps += 1;
        for v in 0..3 {
            for e in w.take_events(v) {
                if let SimEvent::ApiFrame {
                    identity,
                    kind,
                    version,
                    payload,
                    lane,
                    ..
                } = e
                {
                    assert_eq!(lane, Lane::Unary);
                    arrivals.push((
                        v,
                        w.ticks(),
                        Frame {
                            kind,
                            version,
                            payload,
                        },
                        identity.role,
                    ));
                }
            }
        }
    }
    assert_eq!(
        arrivals.len(),
        4,
        "three collector submissions and the client's copy"
    );
    let from_frontend: Vec<&(usize, u64, Frame, PeerRole)> = arrivals
        .iter()
        .filter(|(_, _, _, role)| *role == PeerRole::Frontend)
        .collect();
    assert_eq!(from_frontend.len(), 3);
    let voters: std::collections::BTreeSet<usize> =
        from_frontend.iter().map(|(v, _, _, _)| *v).collect();
    assert_eq!(
        voters,
        (0..3).collect(),
        "every voter heard the frontend directly"
    );
    let ticks: Vec<u64> = from_frontend.iter().map(|(_, t, _, _)| *t).collect();
    let (min, max) = (ticks.iter().min().unwrap(), ticks.iter().max().unwrap());
    assert!(
        *max - *min <= 1,
        "arrivals in the same tick over symmetric paths: {ticks:?}"
    );
    assert!(*min > sent_at);
    // No voter relayed the submission on any of its links: the frames the
    // voters sent since the handshake are none at all.
    for v in 0..3 {
        assert!(
            w.sent(v).iter().all(|(_, f)| {
                let mut reader = coord_types::wire_v1::FrameReader::new();
                reader.push(f).expect("within the reader bound");
                reader
                    .next_frame()
                    .unwrap()
                    .is_none_or(|fr| fr.kind != KIND_SUBMIT)
            }),
            "voter {v} never forwards a submission"
        );
    }
    // The voters' ingress admits the collector's frame and refuses the
    // client's identical bytes: role-scoped, not a general request path.
    for (_, _, frame, role) in &arrivals {
        assert_eq!(frame.kind, KIND_SUBMIT);
        match role {
            PeerRole::Frontend => {
                let admitted = admitted_from_submit(*role, frame).unwrap();
                assert_eq!(admitted.receipt.session(), SessionId([3; 16]));
            }
            PeerRole::Client => assert_eq!(
                admitted_from_submit(*role, frame).map(|_| ()),
                Err(IngressError::RoleNotAuthorized(PeerRole::Client))
            ),
            other => panic!("{other:?}"),
        }
    }
}
