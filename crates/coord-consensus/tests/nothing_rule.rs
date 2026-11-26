//! task-d121's rule in the selection (C5): a command the selection must
//! carry, whose payload no report holds, is decided as nothing, in each of
//! its three shapes, and only where conditions 1 to 3 hold. Each test
//! selects with `select_ruled`, which applies the rule at once; the wait
//! before it (condition 4) is the campaign's, and condition 5's tests are
//! beside the order check's in `recovery.rs`.

use coord_consensus::{
    BallotConfiguration, CandidateDecisions, CommandRecord, ExecutedFacts, NOTHING_ADMISSION,
    Phase, RecoveryError, RecoveryReport, ReportEntry, SelectionRules, SyncDecision, select_ruled,
};
use coord_types::CommandId;
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ReplicaId};

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn ballot(number: u64, leader: u8) -> Ballot {
    Ballot {
        epoch: ConfigurationEpoch::new(1).unwrap(),
        number,
        leader: r(leader),
    }
}

fn command(i: u8) -> CommandId {
    CommandId(Digest32([i; 32]))
}

/// Facts a command was admitted under, and other facts.
const FACTS: Digest32 = Digest32([0x5a; 32]);
const OTHER: Digest32 = Digest32([0xa5; 32]);

/// The rule on, with what it rests on.
const RULE: SelectionRules = SelectionRules {
    leave_out_unheld: true,
    decide_nothing: true,
};

/// Without it: the same domain before the feature.
const NO_RULE: SelectionRules = SelectionRules {
    leave_out_unheld: true,
    decide_nothing: false,
};

/// Five voters; the candidate `r(1)` campaigns at ballot 2.
fn five() -> BallotConfiguration {
    BallotConfiguration::c2_default(
        ConfigurationEpoch::new(1).unwrap(),
        ballot(2, 1),
        (0..5).map(r).collect(),
    )
    .unwrap()
}

fn entry(
    c: CommandId,
    phase: Phase,
    deps: &[CommandId],
    admission: Digest32,
    payload_present: bool,
) -> ReportEntry {
    ReportEntry {
        command: c,
        phase,
        deps: deps.to_vec(),
        path: Digest32([0; 32]),
        paths: Vec::new(),
        seqnum: 1,
        keys: Vec::new(),
        payload_present,
        admission: Some(admission),
    }
}

/// A report at ballot 2 whose committed ballot is `source`.
fn report(replica: u8, source: u64, entries: Vec<ReportEntry>) -> RecoveryReport {
    RecoveryReport {
        replica: r(replica),
        ballot: ballot(2, 1),
        committed_ballot: ballot(source, 0),
        entries,
    }
}

/// What the candidate holds of its own: decided commands, payloads under
/// facts, and executions.
#[derive(Default)]
struct Candidate {
    decided: Vec<CommandId>,
    payloads: Vec<(CommandId, Digest32)>,
    executed: Vec<(CommandId, ExecutedFacts)>,
}

impl CandidateDecisions for Candidate {
    fn decided(&self, command: &CommandId) -> bool {
        self.decided.contains(command)
    }

    fn executed(&self, command: &CommandId) -> ExecutedFacts {
        self.executed
            .iter()
            .find(|(c, _)| c == command)
            .map_or(ExecutedFacts::No, |(_, f)| f.clone())
    }

    fn holds_payload(&self, command: &CommandId, facts: Option<Digest32>) -> bool {
        self.payloads
            .iter()
            .any(|(c, d)| c == command && facts.is_none_or(|f| f == *d))
    }
}

fn select_with(
    reports: &[RecoveryReport],
    candidate: &Candidate,
    rules: SelectionRules,
) -> Result<SyncDecision, RecoveryError> {
    select_ruled(&five(), reports, |_, _| false, |_| None, candidate, rules)
}

fn select(
    reports: &[RecoveryReport],
    rules: SelectionRules,
) -> Result<SyncDecision, RecoveryError> {
    select_with(reports, &Candidate::default(), rules)
}

/// Three reports at the source: the minimum a selection of five reads.
fn at_source(a: Vec<ReportEntry>, b: Vec<ReportEntry>, c: Vec<ReportEntry>) -> Vec<RecoveryReport> {
    vec![report(1, 1, a), report(2, 1, b), report(3, 1, c)]
}

/// The `Behind` shape: an entry names `D`, which no report holds. With
/// the rule, `D` is a nothing re-proposal; without it the selection leaves
/// the gap the candidate gives up on.
#[test]
fn a_dependency_no_report_holds_is_decided_as_nothing() {
    let (d, x) = (command(1), command(2));
    let reports = at_source(
        vec![entry(x, Phase::Accept, &[d], FACTS, true)],
        vec![entry(x, Phase::Accept, &[d], FACTS, true)],
        Vec::new(),
    );
    let ruled = select(&reports, RULE).expect("selected");
    assert!(
        ruled.reproposed.contains(&d) && ruled.nothing.contains(&d),
        "{ruled:?}"
    );
    assert_eq!(ruled.entries[&x].deps, vec![d], "X keeps its dependencies");
    let plain = select(&reports, NO_RULE).expect("selected");
    assert!(!plain.reproposed.contains(&d) && plain.nothing.is_empty());
    // Condition 3: the candidate committed `D`, or holds its payload.
    for candidate in [
        Candidate {
            decided: vec![d],
            ..Candidate::default()
        },
        Candidate {
            payloads: vec![(d, OTHER)],
            ..Candidate::default()
        },
    ] {
        let kept = select_with(&reports, &candidate, RULE).expect("selected");
        assert!(kept.nothing.is_empty() && !kept.reproposed.contains(&d));
    }
}

/// The payload shape: an acceptance at the source whose payload no report
/// holds. With the rule its admission becomes the marker and the rest of
/// the entry stays as selected; without it the selection fails as
/// `HalfInitialized`.
#[test]
fn an_acceptance_whose_payload_no_report_holds_is_decided_as_nothing() {
    let (a, d) = (command(1), command(2));
    let reports = at_source(
        vec![
            entry(a, Phase::Accept, &[], FACTS, true),
            entry(d, Phase::Accept, &[a], FACTS, false),
        ],
        vec![entry(a, Phase::Accept, &[], FACTS, true)],
        Vec::new(),
    );
    let ruled = select(&reports, RULE).expect("selected");
    let e = &ruled.entries[&d];
    assert_eq!(e.admission, Some(NOTHING_ADMISSION));
    assert_eq!((e.phase, e.deps.clone()), (Phase::Accept, vec![a]));
    assert!(ruled.nothing.is_empty(), "an entry, not a re-proposal");
    assert!(matches!(
        select(&reports, NO_RULE),
        Err(RecoveryError::HalfInitialized { command, .. }) if command == d
    ));
    // Condition 3: the candidate holds the payload under the named facts.
    // Under other facts it holds no supplier.
    let held = Candidate {
        payloads: vec![(d, FACTS)],
        ..Candidate::default()
    };
    assert!(matches!(
        select_with(&reports, &held, RULE),
        Err(RecoveryError::HalfInitialized { command, .. }) if command == d
    ));
    let other = Candidate {
        payloads: vec![(d, OTHER)],
        ..Candidate::default()
    };
    let decided = select_with(&reports, &other, RULE).expect("selected");
    assert_eq!(decided.entries[&d].admission, Some(NOTHING_ADMISSION));
}

/// Condition 1 reads a payload row at any phase under the named facts: a
/// payload held only at a pre-acceptance supplies the command, which the
/// selection carries under its facts (row 21, five voters, seed 518).
/// One under other facts is passed over.
#[test]
fn a_payload_at_a_pre_acceptance_supplies_the_command_and_one_under_other_facts_does_not() {
    let d = command(1);
    let accepted = entry(d, Phase::Accept, &[], FACTS, false);
    let supplied = at_source(
        vec![accepted.clone()],
        vec![entry(d, Phase::PreAccept, &[], FACTS, true)],
        Vec::new(),
    );
    let ruled = select(&supplied, RULE).expect("selected");
    assert_eq!(ruled.entries[&d].admission, Some(FACTS));
    assert!(matches!(
        select(&supplied, NO_RULE),
        Err(RecoveryError::HalfInitialized { .. })
    ));
    let other = at_source(
        vec![accepted],
        vec![entry(d, Phase::PreAccept, &[], OTHER, true)],
        Vec::new(),
    );
    let ruled = select(&other, RULE).expect("selected");
    assert_eq!(ruled.entries[&d].admission, Some(NOTHING_ADMISSION));
}

/// The re-proposal shape: a pre-acceptance no reporter holds a payload
/// for, which an entry waits on, is a nothing re-proposal. Held with a
/// payload anywhere, under any facts, it is re-proposed with it.
#[test]
fn a_reproposal_an_entry_waits_on_whose_payload_no_report_holds_is_decided_as_nothing() {
    let (d, x) = (command(1), command(2));
    let reports = at_source(
        vec![entry(x, Phase::Accept, &[d], FACTS, true)],
        vec![entry(d, Phase::PreAccept, &[], FACTS, false)],
        Vec::new(),
    );
    let ruled = select(&reports, RULE).expect("selected");
    assert!(
        ruled.reproposed.contains(&d) && ruled.nothing.contains(&d),
        "{ruled:?}"
    );
    let plain = select(&reports, NO_RULE).expect("selected");
    assert!(plain.reproposed.contains(&d) && plain.nothing.is_empty());
    let held = at_source(
        vec![entry(x, Phase::Accept, &[d], FACTS, true)],
        vec![entry(d, Phase::PreAccept, &[], FACTS, false)],
        vec![entry(d, Phase::PreAccept, &[], OTHER, true)],
    );
    let ruled = select(&held, RULE).expect("selected");
    assert!(ruled.reproposed.contains(&d) && ruled.nothing.is_empty());
}

/// Narrowed under the rule, `HalfInitialized` fails only on what the
/// selection carries: an acceptance below the source with no payload,
/// which no entry waits on, is left out with no wait. Without the
/// feature it fails as before.
#[test]
fn an_unheld_acceptance_below_the_source_no_entry_waits_on_is_left_out() {
    let d = command(1);
    let reports = vec![
        report(1, 1, Vec::new()),
        report(2, 1, Vec::new()),
        report(3, 0, vec![entry(d, Phase::Accept, &[], FACTS, false)]),
    ];
    let ruled = select(&reports, RULE).expect("selected");
    assert!(!ruled.reproposed.contains(&d) && !ruled.entries.contains_key(&d));
    assert!(ruled.nothing.is_empty());
    assert!(matches!(
        select(&reports, NO_RULE),
        Err(RecoveryError::HalfInitialized { command, .. }) if command == d
    ));
}

/// A nothing at the source is carried as the entry; one below it is
/// re-proposed, with its payload where a report holds one, and otherwise
/// decided as nothing again where an entry waits on it.
#[test]
fn a_nothing_at_the_source_is_carried_and_one_below_it_is_reproposed() {
    let (d, x) = (command(1), command(2));
    let carried = select(
        &at_source(
            vec![entry(d, Phase::Accept, &[], NOTHING_ADMISSION, false)],
            Vec::new(),
            Vec::new(),
        ),
        RULE,
    )
    .expect("selected");
    assert_eq!(carried.entries[&d].admission, Some(NOTHING_ADMISSION));
    assert!(carried.nothing.is_empty(), "carried, not decided");
    let below = |payload: bool| {
        vec![
            report(1, 1, vec![entry(x, Phase::Accept, &[d], FACTS, true)]),
            report(2, 1, Vec::new()),
            report(
                3,
                0,
                vec![entry(d, Phase::Accept, &[], NOTHING_ADMISSION, false)],
            ),
            report(4, 0, vec![entry(d, Phase::PreAccept, &[], FACTS, payload)]),
        ]
    };
    let with = select(&below(true), RULE).expect("selected");
    assert!(
        with.reproposed.contains(&d) && with.nothing.is_empty(),
        "{with:?}"
    );
    let without = select(&below(false), RULE).expect("selected");
    assert!(without.reproposed.contains(&d) && without.nothing.contains(&d));
}

/// Condition 3 reads the candidate's executions: rows not read yet are
/// waited for, and an execution keeps the rule off.
#[test]
fn the_candidates_executions_keep_the_rule_off_or_are_read_first() {
    let (d, x) = (command(1), command(2));
    let reports = at_source(
        vec![entry(x, Phase::Accept, &[d], FACTS, true)],
        vec![entry(d, Phase::PreAccept, &[], FACTS, false)],
        Vec::new(),
    );
    let unread = Candidate {
        executed: vec![(d, ExecutedFacts::Unread)],
        ..Candidate::default()
    };
    assert!(matches!(
        select_with(&reports, &unread, RULE),
        Err(RecoveryError::ExecutedRowsUnread { commands }) if commands == vec![d]
    ));
    let without_row = Candidate {
        executed: vec![(d, ExecutedFacts::WithoutRow)],
        ..Candidate::default()
    };
    assert!(matches!(
        select_with(&reports, &without_row, RULE),
        Err(RecoveryError::ExecutedWithoutRow { command }) if command == d
    ));
    let with = Candidate {
        executed: vec![(
            d,
            ExecutedFacts::With(CommandRecord {
                phase: Phase::Executed,
                deps: Vec::new(),
                keys: Vec::new(),
                payload: Some(FACTS),
                paths: Vec::new(),
                synced_seq: Some(1),
                path: Digest32([0; 32]),
            }),
        )],
        ..Candidate::default()
    };
    let decision = select_with(&reports, &with, RULE).expect("selected");
    assert_eq!(decision.entries[&d].admission, Some(FACTS));
    assert!(decision.nothing.is_empty());
}

/// Condition 2 and its tripwire: an entry at COMMIT under facts whose
/// payload no report holds is never decided as nothing. A decision under
/// facts with nobody holding the payload breaks the premise the rule
/// rests on, so a debug build stops on it.
#[test]
fn a_committed_entry_without_a_payload_is_not_decided_as_nothing() {
    let d = command(1);
    let reports = at_source(
        vec![entry(d, Phase::Commit, &[], FACTS, false)],
        Vec::new(),
        Vec::new(),
    );
    let selected = std::panic::catch_unwind(|| select(&reports, RULE));
    if cfg!(debug_assertions) {
        assert!(selected.is_err(), "the tripwire stops a debug build");
    } else {
        assert!(matches!(
            selected.expect("no panic"),
            Err(RecoveryError::HalfInitialized { command, .. }) if command == d
        ));
    }
    // Without the rule it waits as before.
    assert!(matches!(
        select(&reports, NO_RULE),
        Err(RecoveryError::HalfInitialized { command, .. }) if command == d
    ));
}

/// One copy holding a nothing and another, eligible too, holding facts
/// are two decisions: the rule doesn't merge them.
#[test]
fn a_nothing_and_facts_in_two_eligible_copies_fail_as_incompatible() {
    let d = command(1);
    let reports = at_source(
        vec![entry(d, Phase::Accept, &[], NOTHING_ADMISSION, false)],
        vec![entry(d, Phase::Accept, &[], FACTS, true)],
        Vec::new(),
    );
    assert!(matches!(
        select(&reports, RULE),
        Err(RecoveryError::IncompatibleAdmission { command, .. }) if command == d
    ));
}
