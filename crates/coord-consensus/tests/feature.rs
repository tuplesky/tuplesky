//! task-60 acceptance for the activation half: compatible binaries
//! coexist before activation, activation needs every voter, an old
//! binary refuses active state, and nothing here deactivates anything.

use std::collections::BTreeSet;

use coord_consensus::feature::{
    ActivationError, ActiveFeatures, Support, SupportError, SupportLedger,
};
use coord_consensus::quorum::EpochVoters;
use coord_types::formats::Feature;
use coord_types::ids::{ConfigurationEpoch, ReplicaId};

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn replica(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn voters(count: u8) -> EpochVoters {
    EpochVoters::new(epoch(4), (1..=count).map(replica).collect()).unwrap()
}

fn features(list: &[Feature]) -> BTreeSet<Feature> {
    list.iter().copied().collect()
}

fn report(voter: u8, list: &[Feature]) -> Support {
    Support {
        voter: replica(voter),
        epoch: epoch(4),
        features: features(list),
    }
}

/// Unanimity, not a majority: a voter that cannot take part is not
/// behind, it is incapable, and no amount of catching up changes that.
#[test]
fn a_feature_activates_only_once_every_voter_has_reported_it() {
    let voters = voters(5);
    let mut ledger = SupportLedger::new(voters.clone());
    let mut active = ActiveFeatures::new();

    // A clear majority is not enough. This is the whole difference from
    // a checkpoint floor, which a majority does certify.
    for voter in 1..=3u8 {
        ledger
            .record(&report(voter, &[Feature::CheckpointFloor]))
            .unwrap();
    }
    assert!(ledger.unanimous().is_empty());
    let refused = active.activate(Feature::CheckpointFloor, &ledger, epoch(4));
    assert_eq!(
        refused,
        Err(ActivationError::NotUnanimous {
            feature: Feature::CheckpointFloor,
            missing: vec![replica(4), replica(5)],
        }),
        "a majority activated a capability the rest do not have"
    );
    // And the refusal names the nodes, because the answer is always
    // that somebody has not been upgraded.
    assert_eq!(
        ledger.missing(Feature::CheckpointFloor),
        vec![replica(4), replica(5)]
    );

    // The fourth reports; still not enough.
    ledger
        .record(&report(4, &[Feature::CheckpointFloor]))
        .unwrap();
    assert!(
        active
            .activate(Feature::CheckpointFloor, &ledger, epoch(4))
            .is_err()
    );

    // The fifth. Now it activates, and only the feature every voter
    // reported: the others stay off.
    ledger
        .record(&report(
            5,
            &[Feature::CheckpointFloor, Feature::SealedHandoff],
        ))
        .unwrap();
    assert_eq!(ledger.unanimous(), features(&[Feature::CheckpointFloor]));
    active
        .activate(Feature::CheckpointFloor, &ledger, epoch(4))
        .unwrap();
    assert!(active.is_active(Feature::CheckpointFloor));
    assert!(
        !active.is_active(Feature::SealedHandoff),
        "a feature one voter reported became active"
    );
    assert_eq!(
        active.activate(Feature::SealedHandoff, &ledger, epoch(4)),
        Err(ActivationError::NotUnanimous {
            feature: Feature::SealedHandoff,
            missing: vec![replica(1), replica(2), replica(3), replica(4)],
        })
    );

    // Activating twice is a no-op, and the caller is told: a second
    // activation is usually a second operator.
    assert_eq!(
        active.activate(Feature::CheckpointFloor, &ledger, epoch(4)),
        Err(ActivationError::AlreadyActive)
    );
    assert!(active.is_active(Feature::CheckpointFloor));
}

/// Silence is never assent, and a report may grow but never shrink.
#[test]
fn a_voter_that_has_not_reported_supports_nothing_and_cannot_withdraw() {
    let voters = voters(3);
    let mut ledger = SupportLedger::new(voters);

    // Two of three report everything; the third says nothing at all.
    // The silent voter is exactly the one that might be an old binary,
    // so it counts as supporting nothing.
    for voter in 1..=2u8 {
        ledger.record(&report(voter, &Feature::ALL)).unwrap();
    }
    assert!(ledger.unanimous().is_empty());
    assert_eq!(ledger.missing(Feature::SealedHandoff), vec![replica(3)]);

    ledger.record(&report(3, &Feature::ALL)).unwrap();
    assert_eq!(ledger.unanimous(), features(&Feature::ALL));

    // Growing is an upgrade and is fine; shrinking is not, because a
    // cluster that let a report shrink could activate something and
    // then find a voter claiming it never had it.
    assert_eq!(
        ledger.record(&report(1, &[Feature::CheckpointFloor])),
        Err(SupportError::Withdrawn {
            feature: Feature::SealedHandoff,
        })
    );
    assert_eq!(ledger.unanimous(), features(&Feature::ALL));

    // A report from outside the configuration, or for another one, is
    // not a report at all.
    assert_eq!(
        ledger.record(&Support {
            voter: replica(9),
            ..report(1, &Feature::ALL)
        }),
        Err(SupportError::NotAVoter)
    );
    assert_eq!(
        ledger.record(&Support {
            epoch: epoch(5),
            ..report(1, &Feature::ALL)
        }),
        Err(SupportError::EpochMismatch)
    );
}

/// Compatible binaries coexist before activation, and an old binary
/// refuses active state afterwards. That asymmetry is the whole upgrade
/// story.
#[test]
fn an_old_binary_serves_until_activation_and_refuses_after_it() {
    let old = features(&[Feature::CheckpointFloor]);
    let new = features(&Feature::ALL);

    // Nothing active: every build serves, which is what makes a rolling
    // upgrade possible at all.
    let none = ActiveFeatures::new();
    assert_eq!(none.admits(&old), Ok(()));
    assert_eq!(none.admits(&new), Ok(()));

    // The cluster activates something the old build has.
    let mut active = ActiveFeatures::new();
    let voters = voters(3);
    let mut ledger = SupportLedger::new(voters);
    for voter in 1..=3u8 {
        ledger.record(&report(voter, &Feature::ALL)).unwrap();
    }
    active
        .activate(Feature::CheckpointFloor, &ledger, epoch(4))
        .unwrap();
    assert_eq!(active.admits(&old), Ok(()));

    // And then something it does not. From here the old build refuses,
    // and names what it is missing: the answer an operator needs is
    // "this node is too old for this cluster".
    active
        .activate(Feature::SealedHandoff, &ledger, epoch(4))
        .unwrap();
    assert_eq!(active.admits(&old), Err(vec![Feature::SealedHandoff]));
    assert_eq!(active.admits(&new), Ok(()));

    // There is no way back. Nothing in the type removes a feature, and
    // recovering an active set recovers it whole.
    let recovered = ActiveFeatures::recovered(active.active().clone());
    assert_eq!(recovered.admits(&old), Err(vec![Feature::SealedHandoff]));
    assert_eq!(recovered.active(), active.active());
}

/// A recovered ledger is the ledger: activation after a restart rests
/// on the durable reports and on nothing a process remembered.
#[test]
fn a_recovered_ledger_reaches_the_same_answer() {
    let voters = voters(3);
    let reports: Vec<Support> = (1..=3u8).map(|v| report(v, &Feature::ALL)).collect();
    let recovered = SupportLedger::recovered(voters.clone(), &reports);
    assert_eq!(recovered.unanimous(), features(&Feature::ALL));

    let mut fresh = SupportLedger::new(voters);
    for report in &reports {
        fresh.record(report).unwrap();
    }
    assert_eq!(fresh.unanimous(), recovered.unanimous());
    for feature in Feature::ALL {
        assert_eq!(fresh.missing(feature), recovered.missing(feature));
    }
}
