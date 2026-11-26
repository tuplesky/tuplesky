//! The nothing value (task-d121, C2): a command decided as nothing,
//! carried through the table, the selection and a follower's install and
//! acceptance. Nothing decides one before C5's rule, so each test here
//! puts one in by hand.

use coord_consensus::{
    BallotConfiguration, CandidateDecisions, CommandRecord, ExecutedFacts, NOTHING_ADMISSION,
    Phase, RecoveryError, RecoveryReport, ReportEntry, is_nothing, select, select_decided,
};
use coord_types::identity::Digest32;
use coord_types::ids::{Ballot, ConfigurationEpoch, ReplicaId};
use coord_types::{CommandId, HashDomain};

/// The marker is the domain's digest over `nothing`, written out.
#[test]
fn the_nothing_admission_is_its_domains_digest() {
    assert_eq!(
        NOTHING_ADMISSION,
        HashDomain::DecidedNothing.digest(&[b"nothing"])
    );
    assert!(is_nothing(&NOTHING_ADMISSION));
}

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

/// Facts a command was admitted under: any digest but the nothing.
const FACTS: Digest32 = Digest32([0x5a; 32]);

/// Three voters; the candidate `r(1)` campaigns at ballot 1 from 0.
fn three() -> BallotConfiguration {
    BallotConfiguration::c2_default(
        ConfigurationEpoch::new(1).unwrap(),
        ballot(1, 1),
        (0..3).map(r).collect(),
    )
    .unwrap()
}

fn entry(c: CommandId, phase: Phase, admission: Digest32, payload_present: bool) -> ReportEntry {
    ReportEntry {
        command: c,
        phase,
        deps: Vec::new(),
        path: Digest32([0; 32]),
        paths: Vec::new(),
        seqnum: 1,
        keys: Vec::new(),
        payload_present,
        admission: Some(admission),
    }
}

fn report(replica: u8, entries: Vec<ReportEntry>) -> RecoveryReport {
    RecoveryReport {
        replica: r(replica),
        ballot: ballot(1, 1),
        committed_ballot: ballot(0, 0),
        entries,
    }
}

/// A report holding a nothing at ACCEPT or COMMIT carries it: the
/// nothing has no payload to supply, and is no half-initialized record.
/// The same entry under facts without its payload is one.
#[test]
fn a_reported_nothing_is_carried_and_not_half_initialized() {
    let d = command(1);
    for phase in [Phase::Accept, Phase::Commit] {
        let reports = [
            report(1, Vec::new()),
            report(2, vec![entry(d, phase, NOTHING_ADMISSION, false)]),
        ];
        let decision = select(&three(), &reports).expect("selected");
        let carried = &decision.entries[&d];
        assert_eq!(carried.admission, Some(NOTHING_ADMISSION));
        assert_eq!(carried.phase, phase);
        assert!(decision.reproposed.is_empty());
        assert!(decision.nothing.is_empty(), "only C5's rule fills the set");
        // Control: facts with no payload anywhere.
        let reports = [
            report(1, Vec::new()),
            report(2, vec![entry(d, phase, FACTS, false)]),
        ];
        assert!(matches!(
            select(&three(), &reports),
            Err(RecoveryError::HalfInitialized { command, .. }) if command == d
        ));
    }
}

/// One copy holding a nothing and another holding facts are two
/// decisions of one command: never merged.
#[test]
fn a_nothing_beside_facts_is_two_decisions() {
    let d = command(1);
    let reports = [
        report(1, vec![entry(d, Phase::Accept, FACTS, true)]),
        report(2, vec![entry(d, Phase::Accept, NOTHING_ADMISSION, false)]),
    ];
    assert!(matches!(
        select(&three(), &reports),
        Err(RecoveryError::IncompatibleAdmission { command, .. }) if command == d
    ));
}

/// What the candidate executed, for the check against its executions.
struct Executed(ExecutedFacts);

impl CandidateDecisions for Executed {
    fn decided(&self, _: &CommandId) -> bool {
        false
    }

    fn executed(&self, _: &CommandId) -> ExecutedFacts {
        self.0.clone()
    }
}

fn executed_record(admission: Digest32) -> CommandRecord {
    CommandRecord {
        phase: Phase::Executed,
        deps: Vec::new(),
        keys: Vec::new(),
        payload: Some(admission),
        paths: Vec::new(),
        synced_seq: Some(1),
        path: Digest32([0; 32]),
    }
}

/// A nothing the selection carries is checked against the candidate's
/// own executions: under facts it stops, unread it waits, and its own
/// execution of the nothing, or one without a row, passes.
#[test]
fn a_carried_nothing_is_checked_against_the_candidates_executions() {
    let d = command(1);
    let reports = [
        report(1, Vec::new()),
        report(2, vec![entry(d, Phase::Accept, NOTHING_ADMISSION, false)]),
    ];
    let with = |facts: ExecutedFacts| {
        select_decided(&three(), &reports, |_, _| false, |_| None, &Executed(facts))
    };
    assert!(matches!(
        with(ExecutedFacts::With(executed_record(FACTS))),
        Err(RecoveryError::IncompatibleAdmission { command, first, second })
            if command == d && first == NOTHING_ADMISSION && second == FACTS
    ));
    assert!(matches!(
        with(ExecutedFacts::Unread),
        Err(RecoveryError::ExecutedRowsUnread { commands }) if commands == vec![d]
    ));
    for passes in [
        ExecutedFacts::With(executed_record(NOTHING_ADMISSION)),
        ExecutedFacts::WithoutRow,
        ExecutedFacts::No,
    ] {
        let decision = with(passes.clone()).unwrap_or_else(|e| panic!("{passes:?}: {e:?}"));
        assert_eq!(decision.entries[&d].admission, Some(NOTHING_ADMISSION));
    }
}
