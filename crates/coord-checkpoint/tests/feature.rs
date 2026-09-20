//! task-60 acceptance for the durable half: a support row is a fact
//! about a binary, an activation needs every voter's row, and a build
//! started against a store that has activated something it cannot do
//! refuses before admission.

use std::collections::BTreeSet;

use coord_checkpoint::feature::{
    ACTIVE_KEY, ActiveFeaturesV1, AdmitError, FeatureSupportV1, activate_feature, admit,
    own_support, published_activation, read_support, record_support, support_key,
};
use coord_checkpoint::trim::TrimLimits;
use coord_consensus::quorum::EpochVoters;
use coord_store_api::engine::{LocalEngine, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_store_testkit::model::ModelEngine;
use coord_types::formats::Feature;
use coord_types::ids::{ClusterId, ConfigurationEpoch, DomainId, ReplicaId};

const CLUSTER: ClusterId = ClusterId([1; 16]);
const DOMAIN: DomainId = DomainId([2; 16]);

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

fn replica(n: u8) -> ReplicaId {
    ReplicaId([n; 16])
}

fn voters(count: u8) -> EpochVoters {
    EpochVoters::new(epoch(3), (1..=count).map(replica).collect()).unwrap()
}

fn limits() -> TrimLimits {
    TrimLimits::default()
}

fn apply<E: LocalEngine>(engine: &mut E, update: coord_core::effect::StoreUpdate) {
    let mut tx = engine.begin_write().unwrap();
    match &update.value {
        Some(bytes) => tx.put(update.collection, &update.key, bytes).unwrap(),
        None => tx.delete(update.collection, &update.key).unwrap(),
    }
    tx.commit_durable().unwrap();
}

fn report(voter: u8) -> FeatureSupportV1 {
    FeatureSupportV1::of_this_build(replica(voter), CLUSTER, DOMAIN, epoch(3))
}

/// A support row says what a binary can do, keyed by the voter, and it
/// is derived from the registry rather than configured.
#[test]
fn a_support_row_is_this_builds_own_report() {
    let mut engine = ModelEngine::new();
    let voters = voters(3);
    let offered = report(1);
    assert_eq!(
        offered.features().unwrap(),
        Feature::ALL.iter().copied().collect::<BTreeSet<_>>(),
        "a report is not this build's registry"
    );

    let view = engine.reader().snapshot().unwrap();
    assert_eq!(own_support(&view, &replica(1)).unwrap(), None);
    let update = record_support(&view, &voters, &offered).unwrap();
    assert_eq!(update.key, support_key(&replica(1)));
    drop(view);
    apply(&mut engine, update);

    let view = engine.reader().snapshot().unwrap();
    assert_eq!(own_support(&view, &replica(1)).unwrap(), Some(offered));
    assert_eq!(read_support(&view, &limits()).unwrap().len(), 1);

    // A report from outside the configuration is not a report.
    let stranger = FeatureSupportV1::of_this_build(replica(9), CLUSTER, DOMAIN, epoch(3));
    assert!(record_support(&view, &voters, &stranger).is_err());
}

/// Activation needs every configured voter's row, and says who is
/// missing until it has them.
#[test]
fn an_activation_waits_for_every_voter_and_names_the_ones_missing() {
    let mut engine = ModelEngine::new();
    let voters = voters(3);

    for voter in 1..=2u8 {
        let view = engine.reader().snapshot().unwrap();
        let update = record_support(&view, &voters, &report(voter)).unwrap();
        drop(view);
        apply(&mut engine, update);
    }
    let view = engine.reader().snapshot().unwrap();
    let refused = activate_feature(
        &view,
        &voters,
        CLUSTER,
        DOMAIN,
        Feature::CheckpointFloor,
        &limits(),
    );
    match refused {
        Err(coord_checkpoint::feature::ActivateError::NotUnanimous { feature, missing }) => {
            assert_eq!(feature, Feature::CheckpointFloor);
            assert_eq!(missing, vec![replica(3)]);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        published_activation(&view).unwrap(),
        None,
        "a refused activation wrote something"
    );
    // And nothing is active, so every build still serves: this is the
    // coexistence half of the upgrade story.
    assert!(admit(&view).unwrap().is_empty());
    drop(view);

    // The third reports. Now it activates, and the record names its
    // reporters the way a floor names its signers.
    let view = engine.reader().snapshot().unwrap();
    let update = record_support(&view, &voters, &report(3)).unwrap();
    drop(view);
    apply(&mut engine, update);
    let view = engine.reader().snapshot().unwrap();
    let update = activate_feature(
        &view,
        &voters,
        CLUSTER,
        DOMAIN,
        Feature::CheckpointFloor,
        &limits(),
    )
    .expect("every voter reported");
    assert_eq!(update.key, ACTIVE_KEY);
    drop(view);
    apply(&mut engine, update);

    let view = engine.reader().snapshot().unwrap();
    let record = published_activation(&view).unwrap().expect("activated");
    assert_eq!(record.features, vec![Feature::CheckpointFloor.id()]);
    assert_eq!(record.reporters, vec![replica(1), replica(2), replica(3)]);
    assert_eq!(record.configuration, epoch(3));
    assert_eq!(
        admit(&view).unwrap(),
        [Feature::CheckpointFloor]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );

    // A second activation of the same feature is a no-op and says so.
    assert!(matches!(
        activate_feature(
            &view,
            &voters,
            CLUSTER,
            DOMAIN,
            Feature::CheckpointFloor,
            &limits()
        ),
        Err(coord_checkpoint::feature::ActivateError::AlreadyActive { .. })
    ));
}

/// A build that does not support an active feature refuses the store,
/// and an activation naming a feature this build has never heard of is
/// exactly that case.
#[test]
fn a_build_that_cannot_read_the_active_state_refuses_it() {
    let mut engine = ModelEngine::new();
    // An activation written by a newer build: it names a feature
    // identifier this one does not know. Decoding it to a smaller set
    // would be the dangerous reading -- this build would conclude it
    // may serve precisely when it may not.
    let future = ActiveFeaturesV1 {
        cluster: CLUSTER,
        domain: DOMAIN,
        configuration: epoch(3),
        features: vec![Feature::CheckpointFloor.id(), 0x7fff],
        reporters: vec![replica(1)],
    };
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::CheckpointV1.id(),
        ACTIVE_KEY,
        &future.encode().unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();

    let view = engine.reader().snapshot().unwrap();
    assert!(
        matches!(admit(&view), Err(AdmitError::Engine(_))),
        "an unknown active feature decoded to a smaller set"
    );
    assert!(future.features().is_err());
}
