//! task-d33: a donor serves a voter's executed-history ask at the ballot
//! that voter last synchronized, whenever the donor is synchronized there
//! or later (`Machine::synchronized_at_or_after`, used by
//! `Node::serve_catch_up`).
//!
//! A voter that promised a ballot of its own that the others never
//! followed, and was then refused as behind, asks at the ballot it last
//! synchronized. Donors that answered only at their own ballot left it
//! behind for good once the domain went quiet.

use coord_consensus::{
    BallotConfiguration, ConfigurationIdentity, Follower, FollowerConfig, Leader, LeaderConfig,
    ReplicaRole,
};
use coord_core::effect::PeerId;
use coord_daemon::node::Machine;
use coord_types::ids::{
    Ballot, ClusterId, ConfigurationEpoch, DomainId, ExecutionPosition, ReplicaId,
    ReplicaIncarnation,
};

const FRONTEND: PeerId = PeerId {
    replica: ReplicaId([0xf0; 16]),
    incarnation: ReplicaIncarnation::ZERO,
};

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(1).unwrap()
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: epoch(),
        number,
        leader: r(leader),
    }
}

fn identity(me: u8) -> ConfigurationIdentity {
    ConfigurationIdentity {
        cluster: ClusterId([1; 16]),
        domain: DomainId([2; 16]),
        epoch: epoch(),
        voters: (0..3).map(r).collect(),
        replica: r(me),
        incarnation: ReplicaIncarnation::new(1).unwrap(),
        role: ReplicaRole::Voter,
    }
}

fn quorum(b: Ballot) -> BallotConfiguration {
    BallotConfiguration::c2_default(epoch(), b, (0..3).map(r).collect()).unwrap()
}

#[test]
fn a_donor_serves_a_voter_at_an_earlier_ballot() {
    let b2 = ballot(2, 0);
    let leader = Machine::Leader(Box::new(Leader::new(
        LeaderConfig {
            identity: identity(0),
            quorum: quorum(b2),
            genesis: b2,
            frontend: FRONTEND,
            capacity: 8,
        },
        None,
        ExecutionPosition::ZERO,
    )));
    // At its own ballot, as before, and at every earlier one: what it
    // executed includes everything decided there.
    assert!(leader.synchronized_at_or_after(&b2));
    assert!(leader.synchronized_at_or_after(&ballot(1, 1)));
    assert!(leader.synchronized_at_or_after(&ballot(0, 0)));
    assert!(!leader.synchronized_at(&ballot(1, 1)));
    // Not ahead of itself.
    assert!(!leader.synchronized_at_or_after(&ballot(3, 1)));

    let follower = Machine::Follower(Box::new(Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(b2),
        genesis: b2,
        frontend: FRONTEND,
        capacity: 8,
    })));
    assert!(follower.synchronized_at_or_after(&b2));
    assert!(follower.synchronized_at_or_after(&ballot(1, 2)));
    assert!(!follower.synchronized_at_or_after(&ballot(3, 2)));
    // Another epoch is never served.
    let other = Ballot {
        epoch: ConfigurationEpoch::new(2).unwrap(),
        number: 0,
        leader: r(0),
    };
    assert!(!leader.synchronized_at_or_after(&other));
}
