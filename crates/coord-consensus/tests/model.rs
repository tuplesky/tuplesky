//! Bounded models (task-19; design Sections 4.7-4.9, 21.6): every
//! interleaving of a small vote set, every order of recovery reports, and
//! the normal-operation guards with and without enforcement. Results are
//! frozen as fixtures under `fixtures/counterexamples` (regenerate with
//! `COORD_CONSENSUS_WRITE_FIXTURES=1`), including the counterexamples the
//! oracle finds when a guard is removed or the wrong selection rule is used.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coord_consensus::{
    BallotConfiguration, FastAck, GuardViolation, Learned, Phase, RecoveryError, RecoveryReport,
    ReportEntry, SlowAck, Vote, VoteError, VoteSet, guard_accept, guard_commit, select,
};
use coord_types::identity::Digest32;
use coord_types::ids::*;
use coord_types::logical_v1::{CanonicalOperation, LogicalRequest, PutOp};
use coord_types::{CommandId, RetryKey};
use serde::Serialize;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn cmd(i: u8) -> CommandId {
    let key = RetryKey {
        cluster_id: ClusterId([1; 16]),
        domain_id: DomainId([2; 16]),
        session_id: SessionId([3; 16]),
        client_instance_id: ClientInstanceId([4; 16]),
        request_sequence: RequestSequence::new(u64::from(i) + 1).unwrap(),
    };
    let request = LogicalRequest::new(
        NamespaceId([5; 16]),
        CanonicalOperation::Put(PutOp {
            key: vec![i],
            value: vec![],
            lease: None,
            prev_kv: false,
        }),
    );
    CommandId::derive(&key, &request).unwrap()
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number,
        leader: r(leader),
    }
}

fn config(n: u8, leader: u8, fast: &[u8], number: u64) -> BallotConfiguration {
    BallotConfiguration::c2(
        ConfigurationEpoch::new(1).unwrap(),
        ballot(number, leader),
        (0..n).map(r).collect(),
        fast.iter().map(|i| r(*i)).collect(),
    )
    .unwrap()
}

fn fast(
    replica: u8,
    b: Ballot,
    c: CommandId,
    deps: &[CommandId],
    path: u8,
    seq: Option<u64>,
) -> Vote {
    Vote::Fast(FastAck {
        replica: r(replica),
        ballot: b,
        command: c,
        deps: deps.to_vec(),
        paths: alloc_paths(path),
        path: Digest32([path; 32]),
        admission: coord_core::capability::admission_digest(None, 0),
        seqnum: seq,
    })
}

/// The single conservative key's anchor behind a combined digest.
fn alloc_paths(path: u8) -> Vec<(Vec<u8>, Digest32)> {
    vec![(
        coord_consensus::CONSERVATIVE_KEY.to_vec(),
        Digest32([path; 32]),
    )]
}

fn slow(replica: u8, b: Ballot, c: CommandId) -> Vote {
    Vote::Slow(SlowAck {
        replica: r(replica),
        ballot: b,
        command: c,
        admission: coord_core::capability::admission_digest(None, 0),
    })
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            out.push(tail);
        }
    }
    out
}

fn fixture(name: &str, value: &impl Serialize) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/counterexamples")
        .join(name);
    let mut json = serde_json::to_string_pretty(value).unwrap();
    json.push('\n');
    if std::env::var_os("COORD_CONSENSUS_WRITE_FIXTURES").is_some() {
        std::fs::write(&path, &json).unwrap();
    }
    let frozen = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing frozen fixture {}: {e}", path.display()));
    assert_eq!(frozen, json, "frozen fixture {} differs", path.display());
}

#[derive(Serialize)]
struct LearningScenario {
    name: &'static str,
    permutations: usize,
    /// The decision once every vote arrived (order-independent).
    learned: Option<Learned>,
    /// The distinct decisions first authorized by some prefix of the
    /// deliveries: the point at which a quorum first learns depends on the
    /// order, and a slow decision may precede the fast one for the same
    /// dependencies. Empty when nothing is ever learned.
    first_learned: Vec<Learned>,
    rejected: BTreeMap<String, VoteError>,
}

/// Apply every permutation of `votes`; the counted/rejected sets and the
/// final learned decision must not depend on delivery order. After each
/// accepted vote the learning predicate is queried and the first decision
/// latched, so the frozen model exposes what each order authorizes first.
fn explore(
    name: &'static str,
    cfg: &BallotConfiguration,
    c: CommandId,
    votes: &[(&'static str, Vote)],
) -> LearningScenario {
    let mut result: Option<(Option<Learned>, BTreeMap<String, VoteError>)> = None;
    let mut first_learned: Vec<Learned> = Vec::new();
    let perms = permutations(votes);
    for perm in &perms {
        let mut set = VoteSet::new(cfg.clone(), c);
        let mut rejected = BTreeMap::new();
        let mut latched: Option<Learned> = None;
        for (label, v) in perm {
            match set.add(v.clone()) {
                Ok(()) => {
                    if latched.is_none() {
                        latched = set.learned();
                    }
                }
                Err(e) => {
                    // Which redelivered copy is "second" is a delivery
                    // artifact; record duplicates by replica and kind
                    // instead of label.
                    let key = if e == VoteError::Duplicate {
                        let kind = match v {
                            Vote::Fast(_) => "fast",
                            Vote::Slow(_) => "slow",
                        };
                        format!("duplicate-{kind}-from-{:?}", v.replica())
                    } else {
                        (*label).to_owned()
                    };
                    rejected.insert(key, e);
                }
            }
        }
        let learned = set.learned();
        // Once learned, a decision is never withdrawn by later votes and
        // every decision carries the leader's dependencies.
        if let Some(first) = &latched {
            assert!(
                learned.is_some(),
                "{name}: a learned decision was withdrawn"
            );
            if !first_learned.contains(first) {
                first_learned.push(first.clone());
            }
        } else {
            assert!(learned.is_none(), "{name}: learned without a latch");
        }
        let outcome = (learned, rejected);
        match &result {
            None => result = Some(outcome),
            Some(first) => assert_eq!(first, &outcome, "{name}: order-dependent outcome"),
        }
    }
    first_learned.sort_by_key(|l| format!("{l:?}"));
    let (learned, rejected) = result.unwrap();
    LearningScenario {
        name,
        permutations: perms.len(),
        learned,
        first_learned,
        rejected,
    }
}

#[test]
fn learning_is_order_independent_and_rejects_non_c2_evidence() {
    // Three voters, leader r0, fixed fast set {r0, r1}.
    let cfg = config(3, 0, &[0, 1], 1);
    assert_eq!(cfg.slow_size(), 2);
    assert_eq!(cfg.fast_size(), 2);
    assert!(cfg.fast_quorums_intersect_in_majority());
    let b = ballot(1, 0);
    let c1 = cmd(1);
    let deps = [cmd(0)];

    // Fast path: the leader proposal and r1's matching path, with every
    // kind of invalid evidence interleaved.
    let fast_path = explore(
        "fast-path-with-invalid-evidence",
        &cfg,
        c1,
        &[
            ("leader-proposal", fast(0, b, c1, &deps, 7, Some(0))),
            ("r1-fast-same-path", fast(1, b, c1, &deps, 7, None)),
            ("r2-slow", slow(2, b, c1)),
            ("observer-r9-fast", fast(9, b, c1, &deps, 7, None)),
            ("r1-slow-adoption", slow(1, b, c1)),
            ("r1-fast-redelivered", fast(1, b, c1, &deps, 7, None)),
            ("r2-fast-outside-fast-set", fast(2, b, c1, &deps, 7, None)),
            ("r2-wrong-ballot", slow(2, ballot(2, 0), c1)),
            (
                "r2-other-epoch",
                slow(
                    2,
                    Ballot {
                        epoch: ConfigurationEpoch::new(2).unwrap(),
                        number: 1,
                        leader: r(0),
                    },
                    c1,
                ),
            ),
        ],
    );
    // Forged proposals and redelivered adoptions, in every order.
    let forged = explore(
        "forged-proposal-and-redelivered-adoption",
        &cfg,
        c1,
        &[
            ("leader-proposal", fast(0, b, c1, &deps, 7, Some(0))),
            ("r1-forged-proposal", fast(1, b, c1, &deps, 7, Some(3))),
            ("r2-slow", slow(2, b, c1)),
            ("r2-slow-redelivered", slow(2, b, c1)),
            ("leader-redelivered", fast(0, b, c1, &deps, 7, Some(0))),
            ("leader-without-seqnum", fast(0, b, c1, &deps, 7, None)),
        ],
    );
    assert_eq!(
        forged.rejected["r1-forged-proposal"],
        VoteError::ForgedProposal
    );
    assert_eq!(
        forged.rejected["leader-without-seqnum"],
        VoteError::MissingSequence,
        "the leader assigns the order; a proposal without it never counts"
    );
    assert_eq!(
        forged.rejected[&format!("duplicate-slow-from-{:?}", r(2))],
        VoteError::Duplicate
    );
    assert_eq!(
        forged.rejected[&format!("duplicate-fast-from-{:?}", r(0))],
        VoteError::Duplicate
    );
    assert_eq!(
        forged.learned,
        Some(Learned::Slow {
            deps: deps.to_vec()
        })
    );
    assert_eq!(
        fast_path.learned,
        Some(Learned::Fast {
            deps: deps.to_vec()
        })
    );
    // Order-dependent first learning: when r2's adoption arrives before
    // r1's fast acknowledgement the slow quorum decides first, with the
    // same dependencies; the fast decision follows in the other orders.
    assert_eq!(
        fast_path.first_learned,
        vec![
            Learned::Fast {
                deps: deps.to_vec()
            },
            Learned::Slow {
                deps: deps.to_vec()
            },
        ]
    );
    assert_eq!(
        forged.first_learned,
        vec![Learned::Slow {
            deps: deps.to_vec()
        }]
    );
    assert_eq!(fast_path.rejected["observer-r9-fast"], VoteError::NotAVoter);
    assert_eq!(
        fast_path.rejected["r2-fast-outside-fast-set"],
        VoteError::NotInFastSet
    );
    assert_eq!(
        fast_path.rejected["r2-wrong-ballot"],
        VoteError::WrongBallot
    );
    assert_eq!(
        fast_path.rejected["r2-other-epoch"],
        VoteError::WrongBallot,
        "a ballot of another epoch never counts"
    );
    // Redelivered copies are duplicates whichever arrives second; a
    // replica's fast acknowledgement and its adoption both count once.
    assert_eq!(
        fast_path.rejected[&format!("duplicate-fast-from-{:?}", r(1))],
        VoteError::Duplicate
    );
    assert!(!fast_path.rejected.contains_key("r1-slow-adoption"));

    // Differing dependency path: no fast learning; the leader order is
    // adopted by r1 (equal deps) and r2 (slow ack) -> slow path.
    let slow_path = explore(
        "slow-path-path-disagreement",
        &cfg,
        c1,
        &[
            ("leader-proposal", fast(0, b, c1, &deps, 7, Some(0))),
            ("r1-fast-other-path", fast(1, b, c1, &deps, 8, None)),
            ("r2-slow", slow(2, b, c1)),
        ],
    );
    assert_eq!(
        slow_path.learned,
        Some(Learned::Slow {
            deps: deps.to_vec()
        })
    );

    // An arbitrary fastest majority {r0, r2} is not C2: r2's fast ack is
    // rejected and nothing is learned until an adoption arrives.
    let fastest = explore(
        "arbitrary-fastest-majority-is-not-c2",
        &cfg,
        c1,
        &[
            ("leader-proposal", fast(0, b, c1, &deps, 7, Some(0))),
            ("r2-fast-outside-fast-set", fast(2, b, c1, &deps, 7, None)),
        ],
    );
    assert_eq!(fastest.learned, None);

    // Without the leader proposal nothing is learned, however many acks.
    let no_leader = explore(
        "no-leader-proposal",
        &cfg,
        c1,
        &[
            ("r1-fast", fast(1, b, c1, &deps, 7, None)),
            ("r2-slow", slow(2, b, c1)),
        ],
    );
    assert_eq!(no_leader.learned, None);
    assert!(fastest.first_learned.is_empty() && no_leader.first_learned.is_empty());

    fixture(
        "learning_scenarios.json",
        &vec![fast_path, forged, slow_path, fastest, no_leader],
    );
}

/// JSON-friendly view of a decision (object keys must be strings).
#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
struct SyncSummary {
    ballot: Ballot,
    source_ballot: Ballot,
    entries: Vec<coord_consensus::SyncEntry>,
    reproposed: Vec<CommandId>,
}

fn summarize(d: &coord_consensus::SyncDecision) -> SyncSummary {
    SyncSummary {
        ballot: d.ballot,
        source_ballot: d.source_ballot,
        entries: d.entries.values().cloned().collect(),
        reproposed: d.reproposed.iter().copied().collect(),
    }
}

#[derive(Serialize)]
struct RecoveryScenario {
    name: &'static str,
    permutations: usize,
    /// Selection over every report (order-independent).
    outcome: Result<SyncSummary, RecoveryError>,
    /// The distinct selections made when the first majority of reports
    /// arrived, over every arrival order: recovery proceeds on the first
    /// majority, so a report arriving later than that majority never
    /// takes part in it.
    at_first_majority: Vec<Result<SyncSummary, RecoveryError>>,
    /// What a highest-phase-wins merge across all reports would have
    /// chosen (never used; recorded as the counterexample).
    highest_phase_wins: BTreeMap<String, (Phase, Vec<CommandId>)>,
}

/// Synthetic path evidence: a function of the command and its
/// dependency order, so equal local orders give equal paths and different
/// orders differ (what the per-key hash chains of task-21 guarantee).
fn path_of(c: CommandId, deps: &[CommandId]) -> Digest32 {
    let mut d = [0u8; 32];
    d[0] = c.0.0[0];
    for (i, dep) in deps.iter().enumerate() {
        d[1 + (i % 31)] ^= dep.0.0[0].wrapping_add(i as u8 + 1);
    }
    Digest32(d)
}

fn entry(c: CommandId, phase: Phase, deps: &[CommandId]) -> ReportEntry {
    ReportEntry {
        command: c,
        phase,
        deps: deps.to_vec(),
        path: path_of(c, deps),
        paths: vec![(b"*".to_vec(), path_of(c, deps))],
        seqnum: 0,
        keys: vec![b"*".to_vec()],
        payload_present: true,
        admission: None,
    }
}

fn report(replica: u8, cballot: u64, entries: Vec<ReportEntry>) -> RecoveryReport {
    RecoveryReport {
        replica: r(replica),
        ballot: ballot(2, 1),
        committed_ballot: ballot(cballot, 0),
        entries,
    }
}

/// A report for a new ballot other than the five-voter default.
fn report_for(
    replica: u8,
    new_ballot: Ballot,
    cballot: u64,
    entries: Vec<ReportEntry>,
) -> RecoveryReport {
    RecoveryReport {
        replica: r(replica),
        ballot: new_ballot,
        committed_ballot: ballot(cballot, 0),
        entries,
    }
}

fn naive_merge(reports: &[RecoveryReport]) -> BTreeMap<String, (Phase, Vec<CommandId>)> {
    let mut out: BTreeMap<CommandId, (Phase, Vec<CommandId>)> = BTreeMap::new();
    for rep in reports {
        for e in &rep.entries {
            if e.phase < Phase::Accept {
                continue;
            }
            match out.get(&e.command) {
                Some((p, _)) if *p >= e.phase => {}
                _ => {
                    out.insert(e.command, (e.phase, e.deps.clone()));
                }
            }
        }
    }
    out.into_iter()
        .map(|(c, v)| (format!("{c:?}"), v))
        .collect()
}

fn explore_recovery(
    name: &'static str,
    cfg: &BallotConfiguration,
    reports: &[RecoveryReport],
) -> RecoveryScenario {
    let perms = permutations(reports);
    let first = select(cfg, &perms[0]);
    let mut at_first_majority: Vec<Result<SyncSummary, RecoveryError>> = Vec::new();
    for p in &perms {
        assert_eq!(select(cfg, p), first, "{name}: order-dependent selection");
        // What a recovering leader decides the moment its first majority
        // of reports is complete (the schedule the protocol actually runs).
        let majority = &p[..cfg.slow_size().min(p.len())];
        let decided = select(cfg, majority)
            .as_ref()
            .map(summarize)
            .map_err(Clone::clone);
        if !at_first_majority.contains(&decided) {
            at_first_majority.push(decided);
        }
    }
    at_first_majority.sort_by_key(|d| format!("{d:?}"));
    RecoveryScenario {
        name,
        permutations: perms.len(),
        outcome: first.as_ref().map(summarize).map_err(Clone::clone),
        at_first_majority,
        highest_phase_wins: naive_merge(reports),
    }
}

#[test]
fn recovery_selection_is_source_defined_and_order_independent() {
    // Five voters; the new ballot 2 is led by r1 with fast set {r1, r2, r3}.
    let cfg = config(5, 1, &[1, 2, 3], 2);
    let (c1, c2, c3, c4) = (cmd(1), cmd(2), cmd(3), cmd(4));
    let reports = vec![
        report(
            1,
            1,
            vec![
                entry(c1, Phase::Commit, &[]),
                entry(c2, Phase::Accept, &[c1]),
            ],
        ),
        report(
            2,
            1,
            vec![
                entry(c2, Phase::Accept, &[c1]),
                entry(c3, Phase::PreAccept, &[]),
            ],
        ),
        // A lower synchronized ballot: its acceptances are stale state,
        // never candidates -- c3, which only the source pre-accepted, and
        // c4, which the source does not hold.
        report(
            3,
            0,
            vec![
                entry(c3, Phase::Accept, &[c4]),
                entry(c4, Phase::Accept, &[]),
            ],
        ),
    ];
    let scenario = explore_recovery("legitimate-phase-differences", &cfg, &reports);
    let decision = select(&cfg, &reports).unwrap();
    assert_eq!(decision.source_ballot, ballot(1, 0));
    assert_eq!(decision.entries[&c1].phase, Phase::Commit);
    assert_eq!(decision.entries[&c2].phase, Phase::Accept);
    assert_eq!(decision.entries[&c2].deps, vec![c1]);
    assert!(
        !decision.entries.contains_key(&c3),
        "pre-accepted only: re-proposed"
    );
    assert!(
        !decision.entries.contains_key(&c4),
        "lower ballot: re-proposed"
    );
    assert_eq!(decision.reproposed, BTreeSet::from([c3, c4]));
    assert_eq!(scenario.outcome.as_ref().unwrap(), &summarize(&decision));
    // The wrong rule would have adopted c3 with the stale dependencies.
    assert_eq!(
        scenario.highest_phase_wins[&format!("{c3:?}")],
        (Phase::Accept, vec![c4]),
        "the recorded counterexample of highest-phase-wins"
    );

    // A commit is final whatever ballot it was reached in (task-d12): a
    // lower synchronized ballot's COMMIT is selected, and one under other
    // dependencies than the source's acceptance is the alarm it should be.
    let mut committed_below = reports.clone();
    committed_below[2]
        .entries
        .push(entry(c1, Phase::Commit, &[]));
    let decision_below = select(&cfg, &committed_below).unwrap();
    assert_eq!(decision_below.entries[&c1].phase, Phase::Commit);
    let mut disagreeing = reports.clone();
    disagreeing[2].entries.push(entry(c2, Phase::Commit, &[]));
    let below = explore_recovery("below-source-commit-disagrees", &cfg, &disagreeing);
    assert!(matches!(
        below.outcome,
        Err(RecoveryError::IncompatibleAccepted { command, .. }) if command == c2
    ));

    // Incompatible accepted candidates at the source ballot stop recovery
    // with the evidence whenever the conflicting report is among the
    // reports considered. Recovery runs on the first majority: in the
    // orders where r4's report arrives after r1, r2 and r3 completed the
    // majority, the selection has already been made from a consistent
    // majority and r4 is a late report, never part of it.
    let mut incompatible = reports.clone();
    incompatible.push(report(4, 1, vec![entry(c2, Phase::Accept, &[])]));
    let bad = explore_recovery("incompatible-accepted-candidates", &cfg, &incompatible);
    assert!(matches!(
        bad.outcome,
        Err(RecoveryError::IncompatibleAccepted { command, .. }) if command == c2
    ));
    assert_eq!(
        bad.at_first_majority.len(),
        2,
        "{:?}",
        bad.at_first_majority
    );
    assert!(bad.at_first_majority.iter().any(|d| matches!(
        d,
        Err(RecoveryError::IncompatibleAccepted { command, .. }) if *command == c2
    )));
    assert!(
        bad.at_first_majority
            .iter()
            .any(|d| d.as_ref() == Ok(&summarize(&decision))),
        "a consistent first majority selects as without the late report"
    );
    assert_eq!(scenario.at_first_majority, vec![Ok(summarize(&decision))]);

    // A half-initialized entry (accepted without a durable payload) is
    // rejected rather than adopted as a no-op.
    let mut half = reports.clone();
    half.push(RecoveryReport {
        replica: r(4),
        ballot: ballot(2, 1),
        committed_ballot: ballot(1, 0),
        entries: vec![ReportEntry {
            command: cmd(5),
            phase: Phase::Accept,
            deps: vec![],
            path: path_of(cmd(5), &[]),
            paths: vec![(b"*".to_vec(), path_of(cmd(5), &[]))],
            seqnum: 0,
            keys: vec![b"*".to_vec()],
            payload_present: false,
            admission: None,
        }],
    });
    let half = explore_recovery("half-initialized-entry", &cfg, &half);
    assert!(matches!(
        half.outcome,
        Err(RecoveryError::HalfInitialized { command, .. }) if command == cmd(5)
    ));

    // Fewer than a majority, a duplicate reporter and an observer's report.
    assert_eq!(
        select(&cfg, &reports[..2]),
        Err(RecoveryError::InsufficientReports { have: 2, need: 3 })
    );
    let mut dup = reports.clone();
    dup.push(reports[0].clone());
    assert_eq!(
        select(&cfg, &dup),
        Err(RecoveryError::DuplicateReport { replica: r(1) })
    );
    let mut observer = reports.clone();
    observer.push(report(9, 1, vec![]));
    assert_eq!(
        select(&cfg, &observer),
        Err(RecoveryError::NotAVoter { replica: r(9) })
    );
    let mut wrong = reports.clone();
    wrong[0].ballot = ballot(3, 1);
    assert_eq!(
        select(&cfg, &wrong),
        Err(RecoveryError::WrongBallot { replica: r(1) })
    );

    fixture("recovery_scenarios.json", &vec![scenario, bad, half, below]);
}

#[test]
fn possible_fast_decisions_are_recovered_from_the_fixed_fast_set() {
    // Three voters; the source ballot 0 is led by r0, whose default fast
    // set is {r0, r1}; ballot 1 is led by r2 and hears from r1 and itself.
    let cfg = config(3, 2, &[2, 0], 1);
    let (c1, c2, c3) = (cmd(1), cmd(2), cmd(3));
    // r1 pre-accepted c1 then c2 in what may be the leader's order (the
    // leader may have replied fast to both); r2 saw them the other way
    // round. The member's order is adopted, in every delivery order.
    let agreed = vec![
        report_for(
            1,
            ballot(1, 2),
            0,
            vec![
                entry(c1, Phase::PreAccept, &[]),
                entry(c2, Phase::PreAccept, &[c1]),
            ],
        ),
        report_for(
            2,
            ballot(1, 2),
            0,
            vec![
                entry(c2, Phase::PreAccept, &[]),
                entry(c1, Phase::PreAccept, &[c2]),
            ],
        ),
    ];
    let scenario = explore_recovery("possible-fast-decision-adopted", &cfg, &agreed);
    let decision = select(&cfg, &agreed).unwrap();
    assert_eq!(decision.entries[&c1].phase, Phase::Accept);
    assert_eq!(decision.entries[&c1].deps, vec![]);
    assert_eq!(decision.entries[&c2].phase, Phase::Accept);
    assert_eq!(decision.entries[&c2].deps, vec![c1]);
    assert!(decision.reproposed.is_empty());
    assert_eq!(scenario.outcome.as_ref().unwrap(), &summarize(&decision));
    // The naive merge keeps no pre-accepted command: it would re-propose
    // both, possibly in r2's order (the recorded counterexample).
    assert!(
        scenario
            .highest_phase_wins
            .get(&format!("{c1:?}"))
            .is_none_or(|(p, _)| *p == Phase::PreAccept)
    );

    // The source leader among the reports: its rows are authoritative and
    // a command it never proposed is re-proposed, whatever r1 holds.
    let with_leader = vec![
        report_for(0, ballot(1, 2), 0, vec![entry(c1, Phase::Accept, &[])]),
        report_for(
            1,
            ballot(1, 2),
            0,
            vec![
                entry(c1, Phase::Accept, &[]),
                entry(c2, Phase::PreAccept, &[c1]),
            ],
        ),
    ];
    let leader_present = explore_recovery("source-leader-present", &cfg, &with_leader);
    let decision = select(&cfg, &with_leader).unwrap();
    assert_eq!(
        decision.entries.keys().copied().collect::<Vec<_>>(),
        vec![c1]
    );
    assert_eq!(decision.reproposed, BTreeSet::from([c2]));
    assert_eq!(
        leader_present.outcome.as_ref().unwrap(),
        &summarize(&decision)
    );

    // An adopted command the member ordered after a candidate, or reached
    // through a different path, proves the member's order is not the
    // leader's: no fast decision was possible, both are re-proposed.
    let inconsistent = vec![
        report_for(
            1,
            ballot(1, 2),
            0,
            vec![
                entry(c3, Phase::PreAccept, &[]),
                entry(c1, Phase::PreAccept, &[c3]),
                entry(c2, Phase::PreAccept, &[c1]),
            ],
        ),
        report_for(2, ballot(1, 2), 0, vec![entry(c1, Phase::Accept, &[])]),
    ];
    let inconsistent_scenario =
        explore_recovery("member-order-differs-from-the-leader", &cfg, &inconsistent);
    let decision = select(&cfg, &inconsistent).unwrap();
    assert_eq!(
        decision.entries.keys().copied().collect::<Vec<_>>(),
        vec![c1]
    );
    assert_eq!(decision.entries[&c1].deps, vec![]);
    assert_eq!(decision.reproposed, BTreeSet::from([c2, c3]));
    assert_eq!(
        inconsistent_scenario.outcome.as_ref().unwrap(),
        &summarize(&decision)
    );

    // Five voters: the source fast set is {r0, r1, r2}; ballot 1 is led by
    // r3 and hears from r1, r2 and r3. Members disagreeing on a path, or
    // one never having seen the command, prove no fast decision.
    let cfg5 = config(5, 3, &[3, 4, 0], 1);
    let disagreeing = vec![
        report_for(
            1,
            ballot(1, 3),
            0,
            vec![
                entry(c1, Phase::PreAccept, &[]),
                entry(c2, Phase::PreAccept, &[c1]),
            ],
        ),
        report_for(
            2,
            ballot(1, 3),
            0,
            vec![
                entry(c3, Phase::PreAccept, &[]),
                entry(c1, Phase::PreAccept, &[c3]),
            ],
        ),
        report_for(3, ballot(1, 3), 0, vec![]),
    ];
    let disagreeing_scenario = explore_recovery("fast-set-members-disagree", &cfg5, &disagreeing);
    let decision = select(&cfg5, &disagreeing).unwrap();
    assert!(decision.entries.is_empty());
    assert_eq!(decision.reproposed, BTreeSet::from([c1, c2, c3]));
    assert_eq!(
        disagreeing_scenario.outcome.as_ref().unwrap(),
        &summarize(&decision)
    );
    let partial = vec![
        report_for(
            1,
            ballot(1, 3),
            0,
            vec![
                entry(c1, Phase::PreAccept, &[]),
                entry(c2, Phase::PreAccept, &[c1]),
            ],
        ),
        report_for(2, ballot(1, 3), 0, vec![entry(c1, Phase::PreAccept, &[])]),
        report_for(3, ballot(1, 3), 0, vec![]),
    ];
    let decision = select(&cfg5, &partial).unwrap();
    assert_eq!(
        decision.entries.keys().copied().collect::<Vec<_>>(),
        vec![c1]
    );
    assert_eq!(decision.reproposed, BTreeSet::from([c2]));

    fixture(
        "possible_fast_scenarios.json",
        &vec![
            scenario,
            leader_present,
            inconsistent_scenario,
            disagreeing_scenario,
        ],
    );
}

/// A minimal replica model for the guard schedule of Section 21.6: phases
/// of commands at one replica, stepping through leader evidence with the
/// guards enforced or deliberately removed.
#[derive(Serialize)]
struct GuardTrace {
    name: &'static str,
    guards_enforced: bool,
    steps: Vec<String>,
    violation_at_step: Option<usize>,
    oracle_finding: Option<String>,
}

fn run_guard_schedule(enforce: bool) -> GuardTrace {
    let (c1, c2) = (cmd(1), cmd(2));
    let mut phases: BTreeMap<CommandId, Phase> = BTreeMap::new();
    let mut trace = GuardTrace {
        name: "leader-evidence-before-dependency-readiness",
        guards_enforced: enforce,
        steps: Vec::new(),
        violation_at_step: None,
        oracle_finding: None,
    };
    // Step 1: c1's initialization is paused: a descriptor exists, no
    // payload (Start). Step 2: c2 (conflicting, depends on c1) is
    // pre-accepted. Step 3: the leader's proposal for c2 arrives with
    // deps [c1] -> ACCEPT requires c1 in ACCEPT/COMMIT. Step 4: quorum
    // for c2 -> COMMIT requires c1 committed.
    phases.insert(c1, Phase::Start);
    trace
        .steps
        .push("c1 descriptor without payload (Start)".into());
    phases.insert(c2, Phase::PreAccept);
    trace.steps.push("c2 pre-accepted with deps [c1]".into());
    let phase_of = |m: &BTreeMap<CommandId, Phase>, c: &CommandId| m.get(c).copied();
    let accept = guard_accept(&[c1], |c| phase_of(&phases, c));
    trace.steps.push(format!(
        "leader proposal for c2: guard_accept -> {accept:?}"
    ));
    if enforce {
        if accept.is_err() {
            trace.violation_at_step = Some(3);
            return trace;
        }
    } else {
        phases.insert(c2, Phase::Accept);
        trace.steps.push("guard removed: c2 -> Accept".into());
    }
    let commit = guard_commit(&[c1], |c| phase_of(&phases, c));
    trace
        .steps
        .push(format!("quorum for c2: guard_commit -> {commit:?}"));
    if !enforce {
        phases.insert(c2, Phase::Commit);
        trace.steps.push("guard removed: c2 -> Commit".into());
    }
    // The oracle: no command may be COMMIT while a direct dependency is
    // below ACCEPT at this replica.
    for (c, p) in &phases {
        if *p >= Phase::Commit {
            let deps = if *c == c2 { vec![c1] } else { vec![] };
            for d in deps {
                if phases.get(&d).copied().unwrap_or(Phase::Start) < Phase::Accept {
                    trace.oracle_finding = Some(format!(
                        "{c:?} committed while dependency {d:?} is {:?}",
                        phases[&d]
                    ));
                }
            }
        }
    }
    trace
}

#[test]
fn guards_reject_premature_phases_and_the_oracle_detects_their_removal() {
    let enforced = run_guard_schedule(true);
    assert_eq!(enforced.violation_at_step, Some(3));
    assert!(enforced.oracle_finding.is_none());
    let removed = run_guard_schedule(false);
    assert_eq!(removed.violation_at_step, None);
    assert!(
        removed.oracle_finding.is_some(),
        "the oracle must detect the removed guard"
    );
    // Unknown dependency (a pending-ingress placeholder that was never
    // registered) is a distinct violation.
    assert_eq!(
        guard_accept(&[cmd(7)], |_| None),
        Err(GuardViolation::DependencyUnknown { dep: cmd(7) })
    );
    fixture(
        "guard_removed_counterexample.json",
        &vec![enforced, removed],
    );
}

#[test]
fn quorum_policy_matches_the_design_table() {
    // 3 voters: slow 2, C2 fast 2, C1 fast 3. 5 voters: 3, 3, 4.
    for (n, slow, c2, c1) in [(3u8, 2usize, 2usize, 3usize), (5, 3, 3, 4)] {
        let fast: Vec<u8> = (0..c2 as u8).collect();
        let cfg = config(n, 0, &fast, 1);
        assert_eq!(cfg.slow_size(), slow);
        assert_eq!(cfg.fast_size(), c2);
        let c1cfg = BallotConfiguration::c1(
            ConfigurationEpoch::new(1).unwrap(),
            ballot(1, 0),
            (0..n).map(r).collect(),
        )
        .unwrap();
        assert_eq!(c1cfg.fast_size(), c1);
        assert!(c1cfg.fast_quorums_intersect_in_majority());
    }
    // A fast set without the leader, of the wrong size, or with a
    // non-voter is refused; so is a leader outside the voters.
    let epoch = ConfigurationEpoch::new(1).unwrap();
    let voters: BTreeSet<ReplicaId> = (0..3).map(r).collect();
    assert_eq!(
        BallotConfiguration::c2(epoch, ballot(1, 0), voters.clone(), [r(1), r(2)].into()),
        Err(coord_consensus::ConfigurationError::FastSetExcludesLeader)
    );
    assert_eq!(
        BallotConfiguration::c2(
            epoch,
            ballot(1, 0),
            voters.clone(),
            [r(0), r(1), r(2)].into()
        ),
        Err(coord_consensus::ConfigurationError::FastSetNotMajority)
    );
    assert_eq!(
        BallotConfiguration::c2(epoch, ballot(1, 0), voters.clone(), [r(0), r(9)].into()),
        Err(coord_consensus::ConfigurationError::FastSetNotVoters)
    );
    assert_eq!(
        BallotConfiguration::c2(epoch, ballot(1, 9), voters, [r(9), r(0)].into()),
        Err(coord_consensus::ConfigurationError::LeaderNotVoter)
    );
}

#[test]
fn publication_obligations_follow_the_durability_table() {
    use coord_consensus::{DurableRecord, Publication};
    assert!(
        Publication::FastAck
            .requires()
            .contains(&DurableRecord::Vote)
    );
    assert!(
        Publication::FastAck
            .requires()
            .contains(&DurableRecord::PathEvidence)
    );
    assert!(
        Publication::PromiseOrRecoveryResponse
            .requires()
            .contains(&DurableRecord::RecoveryStateAtCut)
    );
    assert!(
        Publication::SlowAck
            .requires()
            .contains(&DurableRecord::AdoptedLeaderOrder)
    );
    assert!(
        Publication::FinalizedResult
            .requires()
            .contains(&DurableRecord::ApplicationOutcome)
    );
    assert!(
        Publication::CheckpointReady
            .requires()
            .contains(&DurableRecord::RecoveryFloor)
    );
    assert_eq!(
        Publication::LeaderReply.requires(),
        &[DurableRecord::ProposalState]
    );
}

#[test]
fn decoded_configurations_are_validated_like_constructed_ones() {
    // A five-voter C2 configuration whose fast set is the leader alone
    // would make a single acknowledgement a fast quorum; the constructor
    // rejects it and so does decoding the same shape.
    let voters: BTreeSet<ReplicaId> = (0..5).map(r).collect();
    let leader_only: BTreeSet<ReplicaId> = BTreeSet::from([r(0)]);
    assert_eq!(
        BallotConfiguration::c2(
            ConfigurationEpoch::new(1).unwrap(),
            ballot(1, 0),
            voters.clone(),
            leader_only.clone(),
        )
        .unwrap_err(),
        coord_consensus::ConfigurationError::FastSetNotMajority
    );
    let good = config(5, 0, &[0, 1, 2], 1);
    let encoded = serde_json::to_value(&good).unwrap();
    let decoded: BallotConfiguration = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(decoded, good);
    let mut forged = encoded;
    forged["fast_set"] = serde_json::to_value(&leader_only).unwrap();
    let err = serde_json::from_value::<BallotConfiguration>(forged).unwrap_err();
    assert!(err.to_string().contains("FastSetNotMajority"), "{err}");
    let mut stranger = serde_json::to_value(&good).unwrap();
    stranger["fast_set"] = serde_json::to_value(BTreeSet::from([r(0), r(1), r(9)])).unwrap();
    assert!(serde_json::from_value::<BallotConfiguration>(stranger).is_err());
}

/// Checklist P5/P6 (task-d19): a fast-set member's fast acknowledgement
/// does not count toward the slow majority, and a slow decision made of
/// adoptions survives any recovering majority with its dependencies.
///
/// Five voters, fast set {r0, r1, r2}. Counting r1's fast acknowledgement
/// as an adoption made x learned from r0, r1 and r3; then r2, recovering
/// from r1, r2 and r4, held only r1's PRE-ACCEPT and re-proposed x with
/// new dependencies, although the collector may already have released it.
#[test]
fn a_slow_decision_counting_a_fast_ack_survives_recovery() {
    let src = BallotConfiguration::c2_default(
        ConfigurationEpoch::new(1).unwrap(),
        ballot(0, 0),
        (0..5).map(r).collect(),
    )
    .unwrap();
    // Fast set of ballot 0 is {r0, r1, r2}.
    let b0 = ballot(0, 0);
    let (y, x) = (cmd(0), cmd(1));
    let mut set = VoteSet::new(src.clone(), x);
    set.add(fast(0, b0, x, &[y], 7, Some(1))).unwrap();
    // r1, a fast-set member, pre-accepted x with the leader's dependencies
    // but another path: no fast decision, and no adoption either.
    set.add(fast(1, b0, x, &[y], 9, None)).unwrap();
    set.add(slow(3, b0, x)).unwrap();
    assert_eq!(
        set.learned(),
        None,
        "a fast acknowledgement counted as an adoption"
    );
    assert_eq!(set.learned_slow(), None);
    // r1 adopts: now x is learned, and r1 holds x at ACCEPT.
    set.add(slow(1, b0, x)).unwrap();
    assert_eq!(set.learned(), Some(Learned::Slow { deps: vec![y] }));
    // r0 and r3 are gone. r2 leads ballot 1 and hears r1, r2 and r4: the
    // one member of the deciding quorum it hears is r1, at ACCEPT.
    let cfg = BallotConfiguration::c2_default(
        ConfigurationEpoch::new(1).unwrap(),
        ballot(1, 2),
        (0..5).map(r).collect(),
    )
    .unwrap();
    let reports = vec![
        report_for(
            1,
            ballot(1, 2),
            0,
            vec![entry(y, Phase::Accept, &[]), entry(x, Phase::Accept, &[y])],
        ),
        report_for(2, ballot(1, 2), 0, vec![entry(y, Phase::Accept, &[])]),
        report_for(4, ballot(1, 2), 0, vec![entry(y, Phase::Accept, &[])]),
    ];
    let decision = select(&cfg, &reports).unwrap();
    let kept = decision.entries.get(&x);
    assert!(
        kept.is_some_and(|e| e.deps == vec![y]),
        "x was learned with deps [y] but recovery selected {kept:?}, reproposed {:?}",
        decision.reproposed
    );
}

/// What one non-leader voter of the source ballot did with x.
#[derive(Clone, Copy, Debug)]
enum XState {
    /// Nothing reached it.
    None,
    /// Pre-accepted x with the leader's dependencies (`true`) or others.
    Pre(bool),
    /// Adopted the leader's proposal, after a fast acknowledgement with
    /// the leader's dependencies, with others, or none.
    Adopted(Option<bool>),
}

const X_STATES: [XState; 6] = [
    XState::None,
    XState::Pre(true),
    XState::Pre(false),
    XState::Adopted(None),
    XState::Adopted(Some(true)),
    XState::Adopted(Some(false)),
];

/// task-d19's bounded model, joining learning with selection at three and
/// five voters: every combination of what the non-leaders did with one
/// command x, whose leader ordered it after y; for every combination whose
/// vote set learns x, every majority of reports, under every new leader
/// among them, selects x with the learned dependencies.
#[test]
fn every_learned_decision_is_selected_by_every_recovering_majority() {
    let (y, x) = (cmd(0), cmd(1));
    let leader_deps = vec![y];
    let other_deps: Vec<CommandId> = Vec::new();
    let mut checked = 0u32;
    for n in [3u8, 5] {
        let src = BallotConfiguration::c2_default(
            ConfigurationEpoch::new(1).unwrap(),
            ballot(0, 0),
            (0..n).map(r).collect(),
        )
        .unwrap();
        let others = usize::from(n - 1);
        let combos = X_STATES.len().pow(others as u32);
        for code in 0..combos {
            let mut states = Vec::new();
            let mut c = code;
            for _ in 0..others {
                states.push(X_STATES[c % X_STATES.len()]);
                c /= X_STATES.len();
            }
            let b0 = ballot(0, 0);
            let mut set = VoteSet::new(src.clone(), x);
            set.add(fast(0, b0, x, &leader_deps, 7, Some(1))).unwrap();
            for (i, s) in states.iter().enumerate() {
                let replica = (i + 1) as u8;
                let fast_eligible = src.fast_eligible(&r(replica));
                let pre = match s {
                    XState::Pre(same) | XState::Adopted(Some(same)) => Some(*same),
                    _ => None,
                };
                if let (Some(same), true) = (pre, fast_eligible) {
                    let (deps, path) = if same {
                        (&leader_deps, 7)
                    } else {
                        (&other_deps, 9)
                    };
                    set.add(fast(replica, b0, x, deps, path, None)).unwrap();
                }
                if matches!(s, XState::Adopted(_)) {
                    set.add(slow(replica, b0, x)).unwrap();
                }
            }
            let Some(learned) = set.learned() else {
                continue;
            };
            let entry_of = |replica: u8| -> Option<ReportEntry> {
                if replica == 0 {
                    return Some(entry(x, Phase::Accept, &leader_deps));
                }
                match states[usize::from(replica) - 1] {
                    XState::None => None,
                    XState::Pre(true) => Some(entry(x, Phase::PreAccept, &leader_deps)),
                    XState::Pre(false) => Some(entry(x, Phase::PreAccept, &other_deps)),
                    XState::Adopted(_) => Some(entry(x, Phase::Accept, &leader_deps)),
                }
            };
            for mask in 0u32..(1 << n) {
                let members: Vec<u8> = (0..n).filter(|i| mask & (1 << i) != 0).collect();
                if members.len() < src.slow_size() {
                    continue;
                }
                for &new_leader in &members {
                    let cfg = BallotConfiguration::c2_default(
                        ConfigurationEpoch::new(1).unwrap(),
                        ballot(1, new_leader),
                        (0..n).map(r).collect(),
                    )
                    .unwrap();
                    let reports: Vec<RecoveryReport> = members
                        .iter()
                        .map(|&m| {
                            let mut entries = vec![entry(y, Phase::Accept, &[])];
                            entries.extend(entry_of(m));
                            report_for(m, ballot(1, new_leader), 0, entries)
                        })
                        .collect();
                    let decision = select(&cfg, &reports).unwrap_or_else(|e| {
                        panic!("n={n} states={states:?} members={members:?}: {e:?}")
                    });
                    let kept = decision.entries.get(&x);
                    assert!(
                        kept.is_some_and(|e| e.deps == learned.deps()),
                        "n={n} states={states:?} learned {learned:?}, members {members:?} \
                         under r{new_leader} selected {kept:?}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 1000, "{checked}");
}

/// A pre-accept as a reporter holds it: `path` is set apart from `deps`,
/// as a follower's may be once the leader's path was recorded for it.
fn pre_accept_with(c: CommandId, deps: &[CommandId], path: u8) -> ReportEntry {
    ReportEntry {
        path: Digest32([path; 32]),
        paths: vec![(b"*".to_vec(), Digest32([path; 32]))],
        ..entry(c, Phase::PreAccept, deps)
    }
}

/// task-d34 (found by the protocol simulator): a fast acknowledgement with
/// the leader's path but other dependencies decides nothing fast.
///
/// A follower that received the leader's proposal before the payload has
/// the leader's digests in its logs; when the payload arrives, its fast
/// acknowledgement carries the leader's path and its own local
/// dependencies. Counting the path alone learned the command fast with the
/// leader's dependencies, which no member of the fast quorum recorded.
#[test]
fn a_fast_acknowledgement_with_the_leaders_path_and_other_dependencies_decides_nothing() {
    let cfg = config(3, 0, &[0, 1], 0);
    let b0 = ballot(0, 0);
    let (x, y, z) = (cmd(1), cmd(2), cmd(3));
    let mut set = VoteSet::new(cfg, x);
    set.add(fast(0, b0, x, &[y], 7, Some(1))).unwrap();
    set.add(fast(1, b0, x, &[z], 7, None)).unwrap();
    assert_eq!(set.learned(), None);
    // With the leader's dependencies too, it is a fast decision.
    let mut set = VoteSet::new(config(3, 0, &[0, 1], 0), x);
    set.add(fast(0, b0, x, &[y], 7, Some(1))).unwrap();
    set.add(fast(1, b0, x, &[y], 7, None)).unwrap();
    assert_eq!(set.learned(), Some(Learned::Fast { deps: vec![y] }));
}

/// task-d34 (found by the protocol simulator): the source leader's own
/// report is one of the fast-set members' for the possible-fast rule.
///
/// The leader's reply to the frontend waits for its proposal batch, which
/// records the command at PRE-ACCEPT; its own ACCEPT row is a later batch.
/// A leader that crashed between the two reported the command at
/// PRE-ACCEPT, and a selection that heard it skipped the possible-fast
/// rule altogether, re-proposing a command the collector had learned fast.
#[test]
fn a_fast_decision_survives_a_leader_that_crashed_before_its_accept_row() {
    let cfg = config(3, 1, &[1, 2], 1);
    let x = cmd(1);
    let reports = vec![
        report_for(0, ballot(1, 1), 0, vec![pre_accept_with(x, &[], 7)]),
        report_for(1, ballot(1, 1), 0, vec![pre_accept_with(x, &[], 7)]),
    ];
    let decision = select(&cfg, &reports).unwrap();
    assert_eq!(
        decision.entries.get(&x).map(|e| (e.phase, e.deps.clone())),
        Some((Phase::Accept, vec![])),
        "reproposed {:?}",
        decision.reproposed
    );
}

/// task-d34 (found by the protocol simulator): fast-set members that agree
/// on a path but not on the dependencies make no possible-fast candidate.
#[test]
fn members_agreeing_on_a_path_but_not_on_dependencies_decide_nothing_fast() {
    let cfg = config(5, 3, &[3, 4, 0], 1);
    let (x, y, z) = (cmd(1), cmd(2), cmd(3));
    // Ballot 0's fast set is {r0, r1, r2}. r1 and r2 pre-accepted x with
    // one path and different dependencies; nothing else distinguishes
    // them. The rule used to take the first member's order.
    let reports = vec![
        report_for(
            1,
            ballot(1, 3),
            0,
            vec![entry(y, Phase::Accept, &[]), pre_accept_with(x, &[y], 9)],
        ),
        report_for(2, ballot(1, 3), 0, vec![pre_accept_with(x, &[z], 9)]),
        report_for(3, ballot(1, 3), 0, vec![entry(y, Phase::Accept, &[])]),
    ];
    let decision = select(&cfg, &reports).unwrap();
    assert!(
        !decision.entries.contains_key(&x),
        "{:?}",
        decision.entries.get(&x)
    );
    assert!(decision.reproposed.contains(&x));
}

/// task-d34 (found by the protocol simulator): a candidate is not kept
/// when a conflicting command accepted at the source ballot, and not
/// ordered after it, is absent from the member's records.
///
/// The member's order never placed that command, so its path for the
/// candidate cannot be the leader's. The check used to look the command's
/// keys up in the member's own records, found none, and let the candidate
/// through with the member's dependencies, ahead of a decided command.
#[test]
fn a_candidate_its_member_ordered_without_an_accepted_command_is_not_kept() {
    let cfg = config(3, 2, &[2, 0], 1);
    let (a, f, x) = (cmd(1), cmd(2), cmd(3));
    let reports = vec![
        // r1, in ballot 0's fast set, pre-accepted x after a and never
        // held f.
        report_for(
            1,
            ballot(1, 2),
            0,
            vec![entry(a, Phase::Commit, &[]), pre_accept_with(x, &[a], 5)],
        ),
        // r2 committed f after a at the source ballot.
        report_for(
            2,
            ballot(1, 2),
            0,
            vec![entry(a, Phase::Commit, &[]), entry(f, Phase::Commit, &[a])],
        ),
    ];
    let decision = select(&cfg, &reports).unwrap();
    assert!(
        !decision.entries.contains_key(&x),
        "{:?}",
        decision.entries.get(&x)
    );
    assert!(decision.reproposed.contains(&x));
}

/// The same check does not drop a candidate for a decision of an earlier
/// ballot its member executed and forgot (task-d34).
#[test]
fn a_candidate_whose_member_forgot_an_earlier_ballots_decision_is_kept() {
    let cfg = config(3, 2, &[2, 0], 2);
    let (b, a, x) = (cmd(1), cmd(2), cmd(3));
    let reports = vec![
        // r1 synchronized ballot 1 (led by r0; fast set {r0, r1}), forgot
        // b, committed a, and pre-accepted x after a.
        report_for(
            1,
            ballot(2, 2),
            1,
            vec![entry(a, Phase::Commit, &[b]), pre_accept_with(x, &[a], 5)],
        ),
        // r2 is behind at ballot 0 and reports b committed there.
        report_for(2, ballot(2, 2), 0, vec![entry(b, Phase::Commit, &[])]),
    ];
    let decision = select(&cfg, &reports).unwrap();
    assert_eq!(
        decision.entries.get(&x).map(|e| e.deps.clone()),
        Some(vec![a]),
        "reproposed {:?}",
        decision.reproposed
    );
}

/// Recovery reads the source ballot's fast set from its configuration
/// where it is known (task-d31). Ballot 0 ran with the fast set {r0, r2},
/// not the default {r0, r1}: r2's pre-acceptance may be a fast decision,
/// which the default rule, looking at r1 instead, would re-propose.
#[test]
fn possible_fast_decisions_follow_the_source_ballots_own_fast_set() {
    let cfg = config(3, 1, &[1, 0], 1);
    let source = config(3, 0, &[0, 2], 0);
    let c1 = cmd(1);
    let reports = vec![
        report_for(1, ballot(1, 1), 0, vec![]),
        report_for(2, ballot(1, 1), 0, vec![entry(c1, Phase::PreAccept, &[])]),
    ];
    let known = coord_consensus::select_from(
        &cfg,
        &reports,
        |_, _| false,
        |b| (*b == source.ballot()).then(|| source.clone()),
    )
    .unwrap();
    assert_eq!(
        known.entries.get(&c1).map(|e| e.phase),
        Some(Phase::Accept),
        "a possible fast decision of the source ballot was not kept: {known:?}"
    );
    assert!(known.reproposed.is_empty());
    // Under the default fast set r1 is the member heard, and it holds
    // nothing: the command is re-proposed.
    let default = select(&cfg, &reports).unwrap();
    assert!(!default.entries.contains_key(&c1));
    assert_eq!(default.reproposed, BTreeSet::from([c1]));
    // A configuration of another ballot, epoch or voter set is not taken
    // for the source's.
    let other = config(3, 0, &[0, 2], 7);
    let ignored =
        coord_consensus::select_from(&cfg, &reports, |_, _| false, |_| Some(other.clone()))
            .unwrap();
    assert_eq!(ignored, default);
}
