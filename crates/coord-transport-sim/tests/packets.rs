//! task-32 acceptance: packet loss, reorder, duplication and MTU
//! schedules reproduce from seeds with the same visible outcome as the
//! message-level baseline; the same seed gives the same datagram trace;
//! an MTU below the handshake fails within the bound; a stalled bulk lane
//! leaves control flowing through QUIC flow control alone; a sparse
//! observer and collector topology opens only what the roles need.

use std::collections::BTreeMap;
use std::sync::Arc;

use coord_transport::{BoundIdentity, CloseReason, Lane, Limits, TransportError, evidence_frame};
use coord_transport_sim::{LinkFaults, NodeConfig, PacketWorld, SimEvent};
use coord_transport_testkit::{TestBinder, TestCa, TestIdentity};
use coord_types::ids::{ClusterId, DomainId, ReplicaId, ReplicaIncarnation};
use coord_types::wire_v1::PeerRole;

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn inc() -> ReplicaIncarnation {
    ReplicaIncarnation::new(1).unwrap()
}

struct Fixture {
    ca: TestCa,
    ids: Vec<TestIdentity>,
}

fn fixture(roles: &[PeerRole]) -> Fixture {
    let ca = TestCa::new();
    let ids: Vec<TestIdentity> = roles
        .iter()
        .enumerate()
        .map(|(i, role)| ca.issue(&format!("node-{i}"), r(i as u8), inc(), *role))
        .collect();
    Fixture { ca, ids }
}

/// A fixture whose node identities are the registered ones (the world
/// presents exactly the certificates the binder knows).
fn registered_world(f: &Fixture, seed: u8, limits: Limits) -> (PacketWorld, Vec<BoundIdentity>) {
    let mut binder = TestBinder::new(CLUSTER, DOMAIN);
    let mut nodes = Vec::new();
    let mut expected = Vec::new();
    for id in &f.ids {
        let issued = f.ca.issue(&id.name, id.replica, id.incarnation, id.role);
        binder.register(&issued);
        expected.push(issued.expected());
        nodes.push((issued, id.role));
    }
    let binder = Arc::new(binder);
    let configs = nodes
        .into_iter()
        .map(|(identity, _)| NodeConfig {
            identity,
            roots: f.ca.roots(),
            cluster: CLUSTER,
            domain: DOMAIN,
            binder: binder.clone(),
            limits,
            capabilities: vec![1],
        })
        .collect();
    (PacketWorld::new([seed; 32], configs), expected)
}

fn connected(events: &[SimEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, SimEvent::Connected { .. }))
        .count()
}

/// Two voters exchange frames both ways over a control lane under the
/// given link faults; returns the world after every frame arrived.
fn exchange(seed: u8, faults: LinkFaults, frames: usize) -> PacketWorld {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let (mut w, expected) = registered_world(&f, seed, Limits::default());
    w.set_default_faults(faults);
    w.connect(
        0,
        1,
        PeerRole::Voter,
        Some(inc()),
        Lane::Control,
        "node-1",
        expected[1].clone(),
    )
    .unwrap();
    assert!(
        w.run_until(20_000, |w| w.connections(0) == 1 && w.connections(1) == 1),
        "handshake and negotiation complete under faults"
    );
    for i in 0..frames {
        w.send(
            0,
            r(1),
            inc(),
            Lane::Control,
            evidence_frame(&[0xa0, i as u8]).unwrap(),
        )
        .unwrap();
        w.send(
            1,
            r(0),
            inc(),
            Lane::Control,
            evidence_frame(&[0xb0, i as u8]).unwrap(),
        )
        .unwrap();
    }
    let done = |w: &PacketWorld| {
        let o = w.visible_outcome();
        o.get(&(1, r(0), Lane::Control)).map_or(0, Vec::len) == frames
            && o.get(&(0, r(1), Lane::Control)).map_or(0, Vec::len) == frames
    };
    assert!(
        w.run_until(60_000, done),
        "every frame arrives under faults"
    );
    w
}

#[test]
fn loss_reorder_duplication_and_mtu_schedules_reproduce_and_keep_the_visible_outcome() {
    let baseline = exchange(1, LinkFaults::default(), 8);
    let expected = baseline.visible_outcome();
    let faults = [
        LinkFaults {
            loss_ppm: 150_000,
            ..LinkFaults::default()
        },
        LinkFaults {
            reorder_ppm: 300_000,
            reorder_ticks: 40,
            delay_min: 2,
            delay_max: 30,
            ..LinkFaults::default()
        },
        LinkFaults {
            duplicate_ppm: 200_000,
            ..LinkFaults::default()
        },
        LinkFaults {
            mtu: 1300,
            loss_ppm: 50_000,
            ..LinkFaults::default()
        },
    ];
    for (k, fault) in faults.iter().enumerate() {
        let seed = 10 + k as u8;
        let first = exchange(seed, *fault, 8);
        let (delivered, dropped) = first.datagrams();
        assert!(delivered > 0);
        if fault.loss_ppm > 0 {
            assert!(dropped > 0, "schedule {k} lost datagrams");
        }
        // Message-level visible outcome: every node saw every frame from
        // its peer on the control lane, regardless of packet faults. Order
        // within a lane may differ (independent streams), so compare sets.
        let mut got = first.visible_outcome();
        let mut want = expected.clone();
        for v in got.values_mut().chain(want.values_mut()) {
            v.sort();
        }
        assert_eq!(got, want, "schedule {k} changes the visible outcome");
        // The same seed reproduces the same datagram trace; another seed
        // under the same faults does not.
        let again = exchange(seed, *fault, 8);
        assert_eq!(
            again.trace_digest(),
            first.trace_digest(),
            "schedule {k} is reproducible"
        );
        assert_eq!(again.datagrams(), first.datagrams());
        let other = exchange(seed + 40, *fault, 8);
        assert_ne!(other.trace_digest(), first.trace_digest());
    }
}

#[test]
fn an_mtu_below_the_handshake_fails_within_the_bound() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let (mut w, expected) = registered_world(&f, 3, Limits::default());
    w.set_default_faults(LinkFaults {
        mtu: 600,
        ..LinkFaults::default()
    });
    w.connect(
        0,
        1,
        PeerRole::Voter,
        Some(inc()),
        Lane::Control,
        "node-1",
        expected[1].clone(),
    )
    .unwrap();
    let reached = w.run_until(90_000, |w| w.connections(0) == 1);
    assert!(!reached, "initial packets never fit the path");
    assert_eq!(w.connections(0), 0);
    assert_eq!(w.connections(1), 0);
    let (_, dropped) = w.datagrams();
    assert!(dropped > 0);
}

#[test]
fn a_stalled_bulk_lane_leaves_control_flowing_through_flow_control_alone() {
    let f = fixture(&[PeerRole::Voter, PeerRole::Voter]);
    let mut limits = Limits::default();
    limits.lanes[Lane::Bulk.index()].max_uni_streams = 2;
    limits.lanes[Lane::Bulk.index()].stream_receive_window = 64 * 1024;
    limits.lanes[Lane::Bulk.index()].receive_window = 128 * 1024;
    let (mut w, expected) = registered_world(&f, 5, limits);
    for lane in [Lane::Control, Lane::Bulk] {
        w.connect(
            0,
            1,
            PeerRole::Voter,
            Some(inc()),
            lane,
            "node-1",
            expected[1].clone(),
        )
        .unwrap();
    }
    assert!(w.run_until(20_000, |w| w.connections(0) == 2 && w.connections(1) == 2));
    let _ = w.take_events(1);
    // Node 1 stops reading its bulk lane; node 0 keeps pushing bulk.
    w.stall(1, Lane::Bulk);
    for i in 0..40u8 {
        w.send(
            0,
            r(1),
            inc(),
            Lane::Bulk,
            evidence_frame(&vec![i; 30 * 1024]).unwrap(),
        )
        .unwrap();
    }
    w.run_until(5_000, |_| false);
    let before = w.visible_outcome();
    assert!(
        !before.contains_key(&(1, r(0), Lane::Bulk)),
        "nothing bulk is read while stalled"
    );
    // Control frames still arrive, promptly, on their own connection.
    let t0 = w.tick();
    w.send(
        0,
        r(1),
        inc(),
        Lane::Control,
        evidence_frame(b"vote").unwrap(),
    )
    .unwrap();
    assert!(w.run_until(2_000, |w| {
        w.visible_outcome()
            .get(&(1, r(0), Lane::Control))
            .is_some_and(|v| v.len() == 1)
    }));
    assert!(
        w.tick() - t0 < 1_000,
        "control latency is unaffected by the stalled bulk lane"
    );
    // Resuming the bulk consumer drains the backlog; nothing was lost or
    // buffered beyond the windows.
    w.resume(1, Lane::Bulk);
    assert!(w.run_until(120_000, |w| {
        w.visible_outcome()
            .get(&(1, r(0), Lane::Bulk))
            .is_some_and(|v| v.len() == 40)
    }));
    let closed: Vec<SimEvent> = w.take_events(1);
    let _ = closed;
}

#[test]
fn a_sparse_observer_and_collector_topology_opens_only_what_roles_need() {
    let roles = [
        PeerRole::Voter,
        PeerRole::Voter,
        PeerRole::Voter,
        PeerRole::Observer,
        PeerRole::Frontend,
    ];
    let f = fixture(&roles);
    let (mut w, expected) = registered_world(&f, 7, Limits::default());
    // Voters: a ring of control lanes, not a full mesh of every lane.
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
    // The observer follows one voter on the bulk lane only.
    w.connect(
        3,
        0,
        PeerRole::Observer,
        Some(inc()),
        Lane::Bulk,
        "node-0",
        expected[0].clone(),
    )
    .unwrap();
    // The frontend collector reaches every voter on a unary lane.
    for (v, exp) in expected.iter().enumerate().take(3) {
        w.connect(
            4,
            v,
            PeerRole::Frontend,
            None,
            Lane::Unary,
            &format!("node-{v}"),
            exp.clone(),
        )
        .unwrap();
    }
    assert!(w.run_until(30_000, |w| {
        (0..5).map(|n| w.connections(n)).sum::<usize>() == 2 * 7
    }));
    assert_eq!(w.connections(3), 1, "observer: one bulk link");
    assert_eq!(w.connections(4), 3, "frontend: one unary link per voter");
    assert_eq!(
        w.connections(0),
        4,
        "voter 0: two ring links, the observer, the frontend"
    );
    // Lane admission by role, on both sides.
    assert!(matches!(
        w.connect(
            3,
            1,
            PeerRole::Observer,
            Some(inc()),
            Lane::Unary,
            "node-1",
            expected[1].clone()
        ),
        Err(TransportError::LaneNotAdmitted(Lane::Unary))
    ));
    assert!(matches!(
        w.connect(
            4,
            1,
            PeerRole::Client,
            None,
            Lane::Control,
            "node-1",
            expected[1].clone()
        ),
        Err(TransportError::LaneNotAdmitted(Lane::Control))
    ));
    // A frontend certificate claiming a voter role is rejected by the
    // acceptor after the real handshake.
    w.connect(
        4,
        2,
        PeerRole::Voter,
        Some(inc()),
        Lane::Control,
        "node-2",
        expected[2].clone(),
    )
    .unwrap();
    assert!(w.run_until(20_000, |w| { w.nodes_closed(2) > 0 }));
    let events = w.take_events(2);
    assert!(events.iter().any(|e| matches!(
        e,
        SimEvent::Closed { reason: CloseReason::Rejected(m), .. } if m.contains("RoleNotAuthorized")
    )));
    let per_node: BTreeMap<usize, usize> = (0..5).map(|n| (n, w.connections(n))).collect();
    assert_eq!(per_node[&2], 3, "the rejected connection never counted");
    let _ = connected(&events);
}
