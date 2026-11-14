//! task-d81: a donor serves a voter's executed-history ask at any ballot
//! of its epoch, whichever ballot it is synchronized at
//! (`Machine::serves_catch_up_at`, used by `Node::serve_catch_up`).
//!
//! task-d33 let a donor serve a voter at an earlier ballot than its own.
//! A donor at an earlier ballot than the asker's served nothing, and a
//! voter that alone executed the newest positions, but missed the ballot
//! the others installed, then left them behind for good: its campaign's
//! frontier held them from campaigning until they had executed that far,
//! and only it could serve those positions (protocol simulator 12/5/148
//! and 2/3/281). What it executed is a prefix of the one executed order,
//! valid at any ballot of the epoch.

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
fn a_donor_serves_a_voter_at_any_ballot_of_its_epoch() {
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
    // At its own ballot, at every earlier one (task-d33), and at a later
    // one it never saw (task-d81).
    assert!(leader.serves_catch_up_at(&b2));
    assert!(leader.serves_catch_up_at(&ballot(1, 1)));
    assert!(leader.serves_catch_up_at(&ballot(0, 0)));
    assert!(leader.serves_catch_up_at(&ballot(3, 1)));
    assert!(!leader.synchronized_at(&ballot(1, 1)));

    let follower = Machine::Follower(Box::new(Follower::new(FollowerConfig {
        identity: identity(1),
        quorum: quorum(b2),
        genesis: b2,
        frontend: FRONTEND,
        capacity: 8,
    })));
    assert!(follower.serves_catch_up_at(&b2));
    assert!(follower.serves_catch_up_at(&ballot(1, 2)));
    // The voter of 2/3/281: synchronized at an earlier ballot than the
    // ones asking it.
    assert!(follower.serves_catch_up_at(&ballot(3, 2)));
    assert!(!follower.synchronized_at(&ballot(3, 2)));
    // Another epoch is never served.
    let other = Ballot {
        epoch: ConfigurationEpoch::new(2).unwrap(),
        number: 0,
        leader: r(0),
    };
    assert!(!leader.serves_catch_up_at(&other));
    assert!(!follower.serves_catch_up_at(&other));
}
