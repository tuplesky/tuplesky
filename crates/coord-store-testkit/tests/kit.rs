//! The honest model passes the suite; each deliberate misbehavior fails a
//! specific check; scenario fixtures replay to a frozen digest.

use std::path::PathBuf;

use coord_store_api::engine::{LocalEngine, OrderedRead, SnapshotSource, WriteTxn};
use coord_store_api::registry::Collection;
use coord_store_testkit::conformance::{ConformanceHarness, ScriptedOutcome, run_all};
use coord_store_testkit::model::{CommitScript, Misbehavior, ModelEngine, ModelEvent};
use coord_store_testkit::scenario::{StoreScenarioV1, replay};

struct ModelHarness(ModelEngine);

impl ConformanceHarness for ModelHarness {
    type Engine = ModelEngine;
    fn engine(&mut self) -> &mut ModelEngine {
        &mut self.0
    }
    fn script_next_commit(&mut self, outcome: ScriptedOutcome) -> bool {
        self.0.script_commit(match outcome {
            ScriptedOutcome::DefinitelyNotCommitted => CommitScript::DefinitelyNotCommitted,
            ScriptedOutcome::IndeterminateApplied => CommitScript::Indeterminate { applied: true },
            ScriptedOutcome::IndeterminateAbsent => CommitScript::Indeterminate { applied: false },
        });
        true
    }
    fn inject_iterator_error(&mut self, rows: usize) -> bool {
        self.0.inject_iterator_error(rows);
        true
    }
    fn crash_and_reopen(&mut self) {
        self.0.crash_and_reopen();
    }
}

#[test]
fn honest_model_passes_every_check() {
    let mut h = ModelHarness(ModelEngine::new());
    let report = run_all(&mut h);
    assert!(report.all_passed(), "{report:?}");
    assert_eq!(report.results.len(), 7);
    assert!(report.skipped().is_empty());
}

#[test]
fn each_misbehavior_is_detected_by_the_named_check() {
    let cases = [
        (Misbehavior::TornWrites, "transactions"),
        (Misbehavior::MixedSnapshots, "transactions"),
        (Misbehavior::ReversedBounds, "ordered_access"),
        (Misbehavior::SwallowedIteratorErrors, "iterator_errors"),
        (Misbehavior::FalseDurability, "durability"),
        (Misbehavior::EarlyVisibility, "transactions"),
        (Misbehavior::UnpinnedScans, "transactions"),
        (Misbehavior::NoncommitVisibleUntilReopen, "commit_outcomes"),
    ];
    for (misbehavior, expected_check) in cases {
        let mut h = ModelHarness(ModelEngine::misbehaving(&[misbehavior]));
        let report = run_all(&mut h);
        assert!(!report.all_passed(), "{misbehavior:?} went undetected");
        assert!(
            report.failed().contains(&expected_check),
            "{misbehavior:?}: failed {:?}, expected {expected_check}",
            report.failed()
        );
    }
}

#[test]
fn harness_without_fault_hooks_reports_skips_not_passes() {
    struct NoHooks(ModelEngine);
    impl ConformanceHarness for NoHooks {
        type Engine = ModelEngine;
        fn engine(&mut self) -> &mut ModelEngine {
            &mut self.0
        }
        fn script_next_commit(&mut self, _: ScriptedOutcome) -> bool {
            false
        }
        fn inject_iterator_error(&mut self, _: usize) -> bool {
            false
        }
        fn crash_and_reopen(&mut self) {
            self.0.crash_and_reopen();
        }
    }
    let report = run_all(&mut NoHooks(ModelEngine::new()));
    assert!(!report.all_passed());
    assert_eq!(report.skipped(), vec!["iterator_errors", "commit_outcomes"]);
    assert!(report.failed().is_empty());
}

#[test]
fn indeterminate_commits_are_all_or_nothing_and_events_are_distinct() {
    let mut engine = ModelEngine::new();
    engine.script_commit(CommitScript::Indeterminate { applied: true });
    {
        let mut tx = engine.begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"a", b"1").unwrap();
        tx.put(Collection::EventsV1.id(), b"a", b"1").unwrap();
        assert!(tx.commit_durable().is_err());
    }
    engine.crash_and_reopen();
    let view = engine.reader().snapshot().unwrap();
    assert_eq!(
        view.get(Collection::KvCurrentV1.id(), b"a").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        view.get(Collection::EventsV1.id(), b"a").unwrap(),
        Some(b"1".to_vec())
    );
    // A false-durability engine loses the batch and reports it as lost.
    let mut liar = ModelEngine::misbehaving(&[Misbehavior::FalseDurability]);
    {
        let mut tx = liar.begin_write().unwrap();
        tx.put(Collection::KvCurrentV1.id(), b"b", b"2").unwrap();
        tx.commit_durable().unwrap();
    }
    liar.crash_and_reopen();
    let events = liar.events();
    assert!(events.contains(&ModelEvent::Visible { txn: 1 }));
    assert!(
        !events.contains(&ModelEvent::Durable { txn: 1 }),
        "visible is not durable"
    );
    assert!(events.contains(&ModelEvent::Reopened { lost: vec![1] }));
    assert_eq!(
        liar.reader()
            .snapshot()
            .unwrap()
            .get(Collection::KvCurrentV1.id(), b"b")
            .unwrap(),
        None
    );
    // Only one writer at a time.
    let mut engine = ModelEngine::new();
    let tx = engine.begin_write().unwrap();
    drop(tx);
    let tx2 = engine.begin_write().unwrap();
    tx2.abort().unwrap();
}

#[test]
fn scenario_fixture_replays_to_frozen_digest() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/store_scenario_v1.json");
    let mut generated = StoreScenarioV1::generate([0x5a; 32], 40);
    assert!(
        generated
            .steps
            .iter()
            .any(|s| matches!(s, coord_store_testkit::scenario::Step::CrashReopen))
    );
    assert!(
        generated
            .steps
            .iter()
            .any(|s| matches!(s, coord_store_testkit::scenario::Step::Abort))
    );
    let mut engine = ModelEngine::new();
    let outcome = replay(&mut engine, &generated, ModelEngine::crash_and_reopen).unwrap();
    assert!(outcome.matches_oracle);
    assert!(outcome.commits > 20);
    generated.expected_digest = Some(outcome.digest);
    if std::env::var_os("COORD_STORE_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&generated).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: StoreScenarioV1 =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, generated,
        "store scenario fixture drifted; the generator is pinned"
    );
    // Replaying the stored fixture on a fresh engine matches its digest.
    let mut fresh = ModelEngine::new();
    let outcome = replay(&mut fresh, &stored, ModelEngine::crash_and_reopen).unwrap();
    assert_eq!(outcome.matches_expected, Some(true));
    assert!(outcome.matches_oracle);
    // A torn engine does not match the oracle or the digest.
    let mut torn = ModelEngine::misbehaving(&[Misbehavior::TornWrites]);
    let outcome = replay(&mut torn, &stored, ModelEngine::crash_and_reopen).unwrap();
    assert!(!outcome.matches_oracle);
    assert_eq!(outcome.matches_expected, Some(false));
}

#[test]
fn crash_discards_the_pending_transaction_in_replay() {
    use coord_store_testkit::scenario::Step;
    let kv = Collection::KvCurrentV1.id().0;
    let scenario = StoreScenarioV1 {
        schema: "store_scenario_v1".to_owned(),
        seed: [0; 32],
        generator: "hand-written".to_owned(),
        steps: vec![
            Step::Put {
                collection: kv,
                key: b"kept".to_vec(),
                value: b"1".to_vec(),
            },
            Step::Commit,
            Step::Put {
                collection: kv,
                key: b"lost".to_vec(),
                value: b"2".to_vec(),
            },
            Step::CrashReopen,
            Step::Commit,
        ],
        expected_digest: None,
    };
    let mut engine = ModelEngine::new();
    let outcome = replay(&mut engine, &scenario, ModelEngine::crash_and_reopen).unwrap();
    assert!(outcome.matches_oracle);
    assert_eq!(outcome.commits, 2);
    let rows = engine.durable_rows();
    assert!(rows.iter().any(|(_, k, _)| k == b"kept"));
    assert!(
        !rows.iter().any(|(_, k, _)| k == b"lost"),
        "a put pending at the crash must not be committed afterwards: {rows:?}"
    );
}

#[test]
fn replay_rejects_an_engine_whose_scans_never_advance() {
    use coord_store_testkit::scenario::Step;
    let kv = Collection::KvCurrentV1.id().0;
    // More rows than one replay page, so a resume key is needed.
    let mut steps = Vec::new();
    for i in 0..100u32 {
        steps.push(Step::Put {
            collection: kv,
            key: format!("k{i:03}").into_bytes(),
            value: vec![1],
        });
    }
    steps.push(Step::Commit);
    let scenario = StoreScenarioV1 {
        schema: "store_scenario_v1".to_owned(),
        seed: [0; 32],
        generator: "hand-written".to_owned(),
        steps,
        expected_digest: None,
    };
    let mut stuck = ModelEngine::misbehaving(&[Misbehavior::IgnoredResumeKey]);
    let err = replay(&mut stuck, &scenario, ModelEngine::crash_and_reopen).unwrap_err();
    assert!(err.contains("did not advance"), "{err}");
    let mut honest = ModelEngine::new();
    assert!(
        replay(&mut honest, &scenario, ModelEngine::crash_and_reopen)
            .unwrap()
            .matches_oracle
    );
}
