//! task-60 acceptance for the durable half: a support row is a fact
//! about a binary, an activation needs every voter's row, and a build
//! started against a store that has activated something it cannot do
//! refuses before admission.

use std::collections::BTreeSet;

use coord_checkpoint::feature::{
    ACTIVE_KEY, ActivateError, ActiveFeaturesV1, AdmitError, FeatureRows, FeatureSupportV1,
    InstallActivationError, PrepareError, activate_feature, active_features, admit,
    install_activation, own_support, prepare_feature, prepared, promotion_missing,
    published_activation, read_support, record_support, support_key,
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
        Feature::ALL
            .iter()
            .copied()
            .filter(|f| !f.is_marker())
            .collect::<BTreeSet<_>>(),
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

/// A voter retained across a configuration change reports again under
/// the new configuration: its earlier row gives way to the report
/// rather than blocking it, what the earlier row reported still binds,
/// and a row from a configuration later than the one in force is still
/// refused.
#[test]
fn a_retained_voter_reports_again_under_the_next_configuration() {
    let mut engine = ModelEngine::new();
    let old = voters(3);
    let view = engine.reader().snapshot().unwrap();
    let update = record_support(&view, &old, &report(1)).unwrap();
    drop(view);
    apply(&mut engine, update);

    // Configuration 4 keeps voters 1 and 2 and swaps 3 for 4. Voter 1's
    // row is from configuration 3, and it reports again.
    let new = EpochVoters::new(epoch(4), [1, 2, 4].into_iter().map(replica).collect()).unwrap();
    let offered = FeatureSupportV1::of_this_build(replica(1), CLUSTER, DOMAIN, epoch(4));
    let view = engine.reader().snapshot().unwrap();
    let update = record_support(&view, &new, &offered)
        .expect("a retained voter's earlier row blocked its report");
    drop(view);
    apply(&mut engine, update);
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        own_support(&view, &replica(1)).unwrap(),
        Some(offered.clone())
    );

    // A withdrawal across the change is still a withdrawal.
    let later = EpochVoters::new(epoch(5), [1, 2, 4].into_iter().map(replica).collect()).unwrap();
    let shrunk = FeatureSupportV1 {
        configuration: epoch(5),
        features: Vec::new(),
        ..offered
    };
    assert!(
        record_support(&view, &later, &shrunk).is_err(),
        "a membership change let a report shrink"
    );

    // And a row from a later configuration than the one in force is not
    // something an earlier configuration's report may replace.
    assert!(
        record_support(&view, &old, &report(1)).is_err(),
        "a report replaced a row from a later configuration"
    );
}

/// A support row that names another cluster or domain is no evidence
/// for this one, even under a configured voter's key: it is refused
/// rather than counted toward unanimity, and nothing is activated.
#[test]
fn a_support_row_of_another_cluster_is_no_evidence_here() {
    let mut engine = ModelEngine::new();
    let voters = voters(3);
    for voter in 1..=2u8 {
        let view = engine.reader().snapshot().unwrap();
        let update = record_support(&view, &voters, &report(voter)).unwrap();
        drop(view);
        apply(&mut engine, update);
    }
    // The third voter's row, as a copy from another cluster would leave
    // it: right voter, right configuration, wrong cluster.
    let misplaced =
        FeatureSupportV1::of_this_build(replica(3), ClusterId([9; 16]), DOMAIN, epoch(3));
    let mut tx = engine.begin_write().unwrap();
    tx.put(
        Collection::CheckpointV1.id(),
        &support_key(&replica(3)),
        &misplaced.encode().unwrap(),
    )
    .unwrap();
    tx.commit_durable().unwrap();

    let view = engine.reader().snapshot().unwrap();
    assert!(
        matches!(
            activate_feature(
                &view,
                &voters,
                CLUSTER,
                DOMAIN,
                Feature::CheckpointFloor,
                &limits()
            ),
            Err(coord_checkpoint::feature::ActivateError::Engine(_))
        ),
        "a row of another cluster counted toward unanimity"
    );
    assert_eq!(published_activation(&view).unwrap(), None);
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
    // It is refused as too old, naming the id, not as a corrupt record:
    // this is what a real downgrade meets (#227 review).
    match admit(&view) {
        Err(AdmitError::TooOld(too_old)) => {
            assert_eq!(too_old.unknown, vec![0x7fff]);
            assert!(too_old.missing.is_empty(), "{too_old:?}");
            let text = too_old.to_string();
            assert!(
                text.contains("too old") && text.contains("0x7fff"),
                "{text}"
            );
        }
        other => panic!("an unknown active feature was not refused as too old: {other:?}"),
    }
    assert!(future.features().is_err());
}

fn apply_all<E: LocalEngine>(engine: &mut E, updates: Vec<coord_core::effect::StoreUpdate>) {
    let mut tx = engine.begin_write().unwrap();
    for update in &updates {
        match &update.value {
            Some(bytes) => tx.put(update.collection, &update.key, bytes).unwrap(),
            None => tx.delete(update.collection, &update.key).unwrap(),
        }
    }
    tx.commit_durable().unwrap();
}

/// A prepare writes the support row and the marker in one batch; the
/// marker activates nothing, and `admit` reads it as a requirement on this
/// build's own support (task-d99).
#[test]
fn a_prepare_writes_the_report_and_the_marker_and_activates_nothing() {
    let mut engine = ModelEngine::new();
    let voters = voters(3);
    let view = engine.reader().snapshot().unwrap();
    let updates = prepare_feature(&view, &voters, &report(1), Feature::EarlyProposal).unwrap();
    assert_eq!(updates.len(), 2, "the support row and the marker, together");
    drop(view);
    apply_all(&mut engine, updates);

    let view = engine.reader().snapshot().unwrap();
    assert!(prepared(&view, &replica(1), Feature::EarlyProposal).unwrap());
    let record = published_activation(&view).unwrap().unwrap();
    assert_eq!(record.features, vec![Feature::EarlyProposalPrepared.id()]);
    assert!(record.reporters.is_empty());
    assert!(
        admit(&view).unwrap().is_empty(),
        "a marker turned a behaviour on"
    );
    assert!(active_features(&view).unwrap().is_empty());

    // Again: the report is the same and the marker is already there.
    let again = prepare_feature(&view, &voters, &report(1), Feature::EarlyProposal).unwrap();
    assert_eq!(again.len(), 1);

    // A feature with no marker has no guard, and is not prepared.
    assert_eq!(
        prepare_feature(&view, &voters, &report(1), Feature::CheckpointFloor),
        Err(PrepareError::NotActivatable {
            feature: Feature::CheckpointFloor
        })
    );
    // A build that does not report the feature does not prepare it.
    let mut without = report(1);
    without
        .features
        .retain(|f| *f != Feature::EarlyProposal.id());
    assert!(matches!(
        prepare_feature(&view, &voters, &without, Feature::EarlyProposal),
        Err(PrepareError::Refused(_) | PrepareError::Unsupported { .. })
    ));
    // A marker is never activated.
    assert_eq!(
        activate_feature(
            &view,
            &voters,
            CLUSTER,
            DOMAIN,
            Feature::EarlyProposalPrepared,
            &limits()
        ),
        Err(ActivateError::NotActivatable {
            feature: Feature::EarlyProposalPrepared
        })
    );
}

/// A prepared store is refused at start by a build from before task-d99,
/// through that build's own `admit`: it decodes the record with a registry
/// that has no marker id, and an unknown id is corruption to it.
#[test]
fn a_prepared_store_is_refused_by_a_build_that_does_not_know_the_marker() {
    let mut engine = ModelEngine::new();
    let view = engine.reader().snapshot().unwrap();
    let updates = prepare_feature(&view, &voters(3), &report(1), Feature::EarlyProposal).unwrap();
    drop(view);
    apply_all(&mut engine, updates);
    let view = engine.reader().snapshot().unwrap();
    let record = published_activation(&view).unwrap().unwrap();

    // The registry of the build before task-d99: ids 1 to 4. Its `admit`
    // is `features()` over that registry, which refuses an unknown id.
    let old_registry = [0x0001u16, 0x0002, 0x0003, 0x0004];
    let unknown: Vec<u16> = record
        .features
        .iter()
        .copied()
        .filter(|id| !old_registry.contains(id))
        .collect();
    assert_eq!(unknown, vec![Feature::EarlyProposalPrepared.id()]);
    // The same refusal, through this build's own decoding of an id it
    // does not know (`a_build_that_cannot_read_the_active_state_refuses_it`).
    let as_old_build_reads_it = ActiveFeaturesV1 {
        features: vec![0x7fff],
        ..record
    };
    assert!(as_old_build_reads_it.features().is_err());
}

fn prepared_engine(voter: u8, voters: &EpochVoters) -> ModelEngine {
    let mut engine = ModelEngine::new();
    let view = engine.reader().snapshot().unwrap();
    let updates = prepare_feature(&view, voters, &report(voter), Feature::EarlyProposal).unwrap();
    drop(view);
    apply_all(&mut engine, updates);
    engine
}

/// The leader activates over the prepared reports in its own store, and a
/// follower installs that record unchanged only over its own durable
/// report; a store that never prepared refuses it.
#[test]
fn an_activation_installs_only_where_the_voter_prepared() {
    let voters = voters(3);
    // The leader: its own prepare, then each follower's prepared report.
    let mut leader = prepared_engine(1, &voters);
    for voter in [2u8, 3] {
        let view = leader.reader().snapshot().unwrap();
        let update = record_support(&view, &voters, &report(voter)).unwrap();
        drop(view);
        apply(&mut leader, update);
    }
    let view = leader.reader().snapshot().unwrap();
    let update = activate_feature(
        &view,
        &voters,
        CLUSTER,
        DOMAIN,
        Feature::EarlyProposal,
        &limits(),
    )
    .unwrap();
    drop(view);
    apply(&mut leader, update);
    let view = leader.reader().snapshot().unwrap();
    let record = published_activation(&view).unwrap().unwrap();
    assert_eq!(
        record.features,
        vec![
            Feature::EarlyProposal.id(),
            Feature::EarlyProposalPrepared.id()
        ],
        "the activation kept the leader's marker"
    );
    assert_eq!(
        admit(&view).unwrap(),
        [Feature::EarlyProposal].into_iter().collect()
    );
    drop(view);

    // A prepared follower installs it, once.
    let mut follower = prepared_engine(2, &voters);
    let view = follower.reader().snapshot().unwrap();
    let update = install_activation(&view, &replica(2), &record)
        .unwrap()
        .expect("a prepared follower installs the record");
    drop(view);
    apply(&mut follower, update);
    let view = follower.reader().snapshot().unwrap();
    let held = published_activation(&view).unwrap().unwrap();
    assert_eq!(held, record, "the record is installed unchanged");
    assert_eq!(install_activation(&view, &replica(2), &record), Ok(None));
    assert_eq!(
        active_features(&view).unwrap(),
        [Feature::EarlyProposal].into_iter().collect()
    );

    // A store that never prepared refuses it.
    let bare = ModelEngine::new();
    let view = bare.reader().snapshot().unwrap();
    assert_eq!(
        install_activation(&view, &replica(3), &record),
        Err(InstallActivationError::NotPrepared {
            missing: vec![Feature::EarlyProposal]
        })
    );
    // Nor does another domain's record land.
    let follower3 = prepared_engine(3, &voters);
    let view = follower3.reader().snapshot().unwrap();
    let elsewhere = ActiveFeaturesV1 {
        domain: DomainId([9; 16]),
        ..record.clone()
    };
    assert_eq!(
        install_activation(&view, &replica(3), &elsewhere),
        Err(InstallActivationError::OtherDomain)
    );
}

/// A voter that reported and then rolled back: once its prepare is
/// durable, no build without the feature is admitted, and a report it
/// later makes cannot withdraw it.
#[test]
fn a_voter_that_rolls_back_after_preparing_is_refused_not_counted() {
    let voters = voters(3);
    let engine = prepared_engine(1, &voters);
    let view = engine.reader().snapshot().unwrap();
    // A build without `EarlyProposal` reports less, and the shrink is
    // refused, as every withdrawal is.
    let mut without = report(1);
    without
        .features
        .retain(|f| *f != Feature::EarlyProposal.id());
    assert!(record_support(&view, &voters, &without).is_err());
    // And the marker it holds is a requirement such a build fails:
    // `admit`'s rule over the record, with that build's support.
    let record = published_activation(&view).unwrap().unwrap();
    let held = record.active().unwrap();
    let supported: BTreeSet<Feature> = Feature::ALL
        .iter()
        .copied()
        .filter(|f| *f != Feature::EarlyProposal && *f != Feature::EarlyProposalPrepared)
        .collect();
    assert_eq!(
        held.admits(&supported),
        Err(vec![Feature::EarlyProposalPrepared])
    );
}

/// The rows a generation replacement carries, and the check selection
/// makes: a store holding fewer is refused.
#[test]
fn feature_rows_are_carried_and_a_store_without_them_does_not_hold_them() {
    let voters = voters(3);
    let engine = prepared_engine(1, &voters);
    let view = engine.reader().snapshot().unwrap();
    let rows = FeatureRows::read(&view, &replica(1)).unwrap();
    assert!(rows.support.is_some() && rows.activation.is_some());
    assert!(rows.held_by(&view).unwrap());

    let mut staged = ModelEngine::new();
    let empty = staged.reader().snapshot().unwrap();
    assert!(!rows.held_by(&empty).unwrap());
    drop(empty);
    apply_all(&mut staged, rows.updates().unwrap());
    let carried = staged.reader().snapshot().unwrap();
    assert!(rows.held_by(&carried).unwrap());
    assert!(prepared(&carried, &replica(1), Feature::EarlyProposal).unwrap());
    assert!(FeatureRows::default().held_by(&carried).unwrap());
}

/// Promotion waits for an advertisement of every active feature.
#[test]
fn a_learner_without_an_active_feature_is_not_promoted() {
    let active: BTreeSet<Feature> = [Feature::EarlyProposal].into_iter().collect();
    assert_eq!(
        promotion_missing(&active, &BTreeSet::new()),
        vec![Feature::EarlyProposal]
    );
    assert!(promotion_missing(&active, &active).is_empty());
}
