//! Reproducibility, explicit ordering, faulty-actor detection with
//! minimization and saved bundles, and clear rejection of incompatible
//! bundles.

use coord_sim::replay::{
    ActorKind, Fault, ReplayBundleV1, ReplayError, Scenario, minimize, run_scenario,
};
use coord_sim::{BuildIdentity, World};

fn seed(n: u8) -> [u8; 32] {
    [n; 32]
}

#[test]
fn identical_replay_has_identical_trace_and_visible_history() {
    let mut scenario = Scenario::new(seed(1), ActorKind::DurableEcho, 3);
    scenario.network.loss_ppm = 100_000;
    scenario.network.duplicate_ppm = 100_000;
    scenario.faults = vec![
        Fault::Crash { node: 1, tick: 15 },
        Fault::Restart { node: 1, tick: 30 },
        Fault::Cut {
            from: 0,
            to: 2,
            tick: 5,
        },
    ];
    let a = run_scenario(&scenario);
    let b = run_scenario(&scenario);
    assert_eq!(a, b);
    assert!(a.report.steps > 0);
    assert_eq!(a.report.crashes, 1);
    assert!(
        a.violations.is_empty(),
        "the correct actor never acknowledges before durability"
    );
    let mut other = scenario.clone();
    other.seed = seed(2);
    let c = run_scenario(&other);
    assert_ne!(
        a.report.trace_digest, c.report.trace_digest,
        "a different seed is a different schedule"
    );
}

#[test]
fn loss_duplication_and_partitions_do_not_break_the_correct_actor() {
    let mut scenario = Scenario::new(seed(3), ActorKind::DurableEcho, 2);
    scenario.network.loss_ppm = 300_000;
    scenario.network.duplicate_ppm = 300_000;
    scenario.storage.fail_ppm = 100_000;
    scenario.workload.requests = 50;
    scenario.faults = vec![
        Fault::Crash { node: 0, tick: 20 },
        Fault::Restart { node: 0, tick: 40 },
        Fault::Crash { node: 0, tick: 60 },
        Fault::Restart { node: 0, tick: 61 },
    ];
    let outcome = run_scenario(&scenario);
    assert!(outcome.violations.is_empty(), "{:?}", outcome.violations);
    assert_eq!(outcome.report.crashes, 2);
    // Some acknowledgements were delivered despite loss.
    let mut world = World::new(scenario);
    world.run(|_, _| {});
    assert!(!world.client_acks().is_empty());
}

#[test]
fn faulty_actor_is_caught_minimized_and_saved() {
    let mut scenario = Scenario::new(seed(4), ActorKind::EagerEcho, 2);
    scenario.workload.requests = 40;
    scenario.storage.min_delay = 5;
    scenario.storage.max_delay = 9;
    scenario.faults = vec![
        Fault::Cut {
            from: 0,
            to: 1,
            tick: 3,
        },
        Fault::Crash { node: 0, tick: 12 },
        Fault::Restart { node: 0, tick: 20 },
        Fault::Crash { node: 1, tick: 30 },
        Fault::Heal {
            from: 0,
            to: 1,
            tick: 33,
        },
        Fault::Restart { node: 1, tick: 40 },
    ];
    let outcome = run_scenario(&scenario);
    assert!(
        !outcome.violations.is_empty(),
        "the eager actor's ack must be caught after a crash"
    );

    let fails = |s: &Scenario| !run_scenario(s).violations.is_empty();
    let minimal = minimize(scenario.clone(), fails);
    assert!(fails(&minimal));
    assert_eq!(
        minimal.faults.len(),
        1,
        "one crash suffices: {:?}",
        minimal.faults
    );
    assert!(matches!(minimal.faults[0], Fault::Crash { .. }));
    assert!(minimal.workload.requests < scenario.workload.requests);

    // The correct actor under the minimal schedule passes.
    let mut fixed = minimal.clone();
    fixed.actor = ActorKind::DurableEcho;
    assert!(run_scenario(&fixed).violations.is_empty());

    // Save, reload and replay the minimized failure.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("eager-echo-minimal.json");
    let bundle = ReplayBundleV1::record(minimal, "eager echo acknowledges before durability");
    assert!(!bundle.expected.violations.is_empty());
    bundle.save(&path).unwrap();
    let loaded = ReplayBundleV1::load(&path).unwrap();
    assert_eq!(loaded, bundle);
    let replayed = loaded.replay().unwrap();
    assert_eq!(replayed.report.trace_digest, bundle.expected.trace_digest);
    assert!(!replayed.violations.is_empty());
}

#[test]
fn incompatible_bundles_fail_clearly() {
    let bundle =
        ReplayBundleV1::record(Scenario::new(seed(5), ActorKind::DurableEcho, 1), "compat");
    bundle.replay().unwrap();
    let mut wrong_version = bundle.clone();
    wrong_version.format_version = 2;
    assert!(matches!(
        wrong_version.replay(),
        Err(ReplayError::Incompatible {
            field: "format_version",
            ..
        })
    ));
    let mut wrong_crate = bundle.clone();
    wrong_crate.build.crate_version = "0.0.0-other".to_owned();
    assert!(matches!(
        wrong_crate.replay(),
        Err(ReplayError::Incompatible {
            field: "crate_version",
            ..
        })
    ));
    let mut wrong_generator = bundle.clone();
    wrong_generator.build.generator = "xorshift".to_owned();
    assert!(matches!(
        wrong_generator.replay(),
        Err(ReplayError::Incompatible {
            field: "generator",
            ..
        })
    ));
    let mut wrong_lock = bundle.clone();
    wrong_lock.build.lock_digest = Some("00".repeat(32));
    let err = wrong_lock
        .check_compatible(&BuildIdentity::current())
        .unwrap_err();
    assert!(
        matches!(
            err,
            ReplayError::Incompatible {
                field: "lock_digest",
                ..
            }
        ),
        "{err}"
    );
    let mut wrong_format = bundle.clone();
    wrong_format.format = "something-else".to_owned();
    assert!(matches!(
        wrong_format.replay(),
        Err(ReplayError::Incompatible {
            field: "format",
            ..
        })
    ));
    // A tampered expectation is a divergence, not a silent pass.
    let mut tampered = bundle;
    tampered.expected.visible_digest = coord_types::identity::Digest32([0; 32]);
    assert!(matches!(
        tampered.replay(),
        Err(ReplayError::Diverged { .. })
    ));
}

#[test]
fn insertion_ties_are_explicit_in_the_trace() {
    // Two requests scheduled at the same tick keep their insertion order
    // across runs: the trace digest is stable and the client history order
    // matches on repeated runs with gap zero.
    let mut scenario = Scenario::new(seed(6), ActorKind::DurableEcho, 1);
    scenario.workload.min_gap = 0;
    scenario.workload.max_gap = 0;
    scenario.storage.min_delay = 1;
    scenario.storage.max_delay = 1;
    scenario.network.max_delay = 1;
    let mut w1 = World::new(scenario.clone());
    w1.run(|_, _| {});
    let mut w2 = World::new(scenario);
    w2.run(|_, _| {});
    assert_eq!(w1.client_acks(), w2.client_acks());
    let values: Vec<&[u8]> = w1.client_acks().iter().map(|(_, v)| v.as_slice()).collect();
    let expected: Vec<Vec<u8>> = (0..20).map(|i| format!("req-{i}").into_bytes()).collect();
    assert_eq!(
        values,
        expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
    );
}
