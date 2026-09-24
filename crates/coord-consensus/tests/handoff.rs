//! Bounded models of the sealed membership handoff (task-54; design
//! Sections 4.8, 10.3.2, 23 G5).
//!
//! A membership transition is the one operation in the system where a
//! coordinator can die in the middle and the replacement has to work
//! out what already happened. The dangerous failure is not a crash, it
//! is a replacement that believes the wrong thing about how far the
//! last one got -- so every world here runs the transition to some
//! point, throws the coordinator away, and asks `resume` where to carry
//! on from the durable records alone.
//!
//! What every world checks:
//!
//! * **No fence is ever cleared.** Once any voter has sealed, no
//!   evidence at all returns `Stable`. A partial seal is reconciled by
//!   continuing, never by rolling back -- the voters that sealed will
//!   not vote in the old configuration again whatever a coordinator
//!   decides.
//! * **A seal and a cancellation never both certify.** One voter, one
//!   stance, never reversed; so the two majorities that would be needed
//!   cannot both form.
//! * **One terminal state.** A terminal certificate is selected only
//!   after the seal and only from a majority agreeing on one root, and
//!   each sealed voter reports one root once; so a replacement
//!   coordinator that asks for another root is refused, and no two
//!   certificates for one transition name different roots.
//! * **A cancellation outlives its replacement.** A voter that
//!   cancelled this transition, then cancelled another, still refuses a
//!   late seal for this one.
//! * **Resume is monotone.** Evidence only accumulates, and the stage
//!   it justifies never goes backwards.
//! * **An activated successor is reused, never recomputed.**
//!
//! Results are frozen under `fixtures/counterexamples` (regenerate with
//! `COORD_CONSENSUS_WRITE_FIXTURES=1`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coord_consensus::handoff::{
    Evidence, HandoffError, InstallRecord, Stage, Stance, StanceError, StanceLedger, StanceRecord,
    TerminalCertificate, TerminalReport, TerminalReportError, Transition, activate, cancel, resume,
    seal, select_terminal, stances_of,
};
use coord_consensus::quorum::EpochVoters;
use coord_types::identity::Digest32;
use coord_types::ids::{ConfigurationEpoch, ReplicaId};
use serde::Serialize;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch(n: u64) -> ConfigurationEpoch {
    ConfigurationEpoch::new(n).unwrap()
}

const OLD: u64 = 4;
const NEW: u64 = 5;

/// The old voters: replicas 0, 1, 2.
fn old_voters() -> EpochVoters {
    EpochVoters::new(epoch(OLD), (0..3).map(r).collect()).unwrap()
}

/// The successor: replicas 2, 3, 4 -- one shared with the old set,
/// because a handoff that only ever replaced everybody would never
/// exercise a replica holding two roles.
fn successor() -> EpochVoters {
    EpochVoters::new(epoch(NEW), [r(2), r(3), r(4)].into()).unwrap()
}

fn transition(subject: u8) -> Transition {
    Transition {
        from: epoch(OLD),
        to: epoch(NEW),
        subject: Digest32([subject; 32]),
    }
}

/// The transition under test, and a competing one an operator started
/// instead.
const THIS: u8 = 0xa1;
const OTHER: u8 = 0xb2;

fn root(n: u8) -> Digest32 {
    Digest32([n; 32])
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

/// One old voter's script: what it was asked to record, in order.
fn stance_scripts() -> Vec<Vec<(u8, Stance)>> {
    vec![
        vec![],
        vec![(THIS, Stance::Sealed)],
        vec![(THIS, Stance::Cancelled)],
        // A reversal attempt, both ways round.
        vec![(THIS, Stance::Cancelled), (THIS, Stance::Sealed)],
        vec![(THIS, Stance::Sealed), (THIS, Stance::Cancelled)],
        // A stale or competing transition arriving at a voter that has
        // already decided about this one.
        vec![(THIS, Stance::Sealed), (OTHER, Stance::Sealed)],
        vec![(THIS, Stance::Cancelled), (OTHER, Stance::Sealed)],
        // A cancellation replaced by another, then a delayed seal for
        // the first: the cancelled transition must stay cancelled.
        vec![
            (THIS, Stance::Cancelled),
            (OTHER, Stance::Cancelled),
            (THIS, Stance::Sealed),
        ],
    ]
}

/// How far the coordinator got before it died, as a count of stages it
/// managed to make durable: reports, the terminal certificate, the
/// successor's installations, and the activation. Installing and
/// activating are separate levels on purpose -- the gap between them is
/// where a coordinator dies with a successor that holds the state and
/// no authority to serve it.
fn coordinator_progress() -> Vec<usize> {
    (0..5).collect()
}

/// Every assignment of a script to each old voter, crossed with how far
/// the coordinator got.
fn worlds() -> Vec<(Vec<usize>, usize)> {
    let scripts = stance_scripts().len();
    let mut assignments = vec![Vec::new()];
    for _ in 0..3 {
        let mut next = Vec::new();
        for prefix in &assignments {
            for s in 0..scripts {
                let mut w = prefix.clone();
                w.push(s);
                next.push(w);
            }
        }
        assignments = next;
    }
    let mut out = Vec::new();
    for assignment in assignments {
        for progress in coordinator_progress() {
            out.push((assignment.clone(), progress));
        }
    }
    out
}

/// What running one world left durable.
struct World {
    /// Every stance any voter ever succeeded in recording. A
    /// certificate formed from it is durable for ever, so the rules
    /// that stop two certificates forming have to hold over the whole
    /// history and not merely over whatever the rows hold now.
    history: Vec<StanceRecord>,
    stances: Vec<StanceRecord>,
    refusals: Vec<StanceError>,
    fenced: BTreeSet<ReplicaId>,
    reports: Vec<TerminalReport>,
    /// Reports a replacement coordinator obtained by asking the same
    /// voters for a second root. Must stay empty wherever a voter already
    /// reported.
    late_reports: Vec<TerminalReport>,
    terminal_refusals: Vec<TerminalReportError>,
    terminal: Option<TerminalCertificate>,
    installs: Vec<InstallRecord>,
    activation: Option<coord_consensus::handoff::ActivationCertificate>,
}

fn run(assignment: &[usize], progress: usize) -> World {
    let old = old_voters();
    let new = successor();
    let scripts = stance_scripts();
    let mut ledgers = Vec::new();
    let mut refusals = Vec::new();
    // Every stance a voter ever succeeded in recording, not just what
    // its ledger ends up holding. A fence that was durable happened,
    // and a model that only looked at the final row could not tell a
    // rule that forbids clearing one from a rule that clears it
    // quietly.
    let mut ever_sealed: BTreeSet<ReplicaId> = BTreeSet::new();
    let mut history: Vec<StanceRecord> = Vec::new();
    for (i, script) in assignment.iter().enumerate() {
        let mut ledger = StanceLedger::new(r(i as u8), epoch(OLD));
        for (subject, stance) in &scripts[*script] {
            match ledger.record(&old, transition(*subject), *stance) {
                Ok(record) => {
                    history.push(record);
                    if record.stance == Stance::Sealed {
                        ever_sealed.insert(record.voter);
                    }
                }
                Err(e) => refusals.push(e),
            }
        }
        ledgers.push(ledger);
    }
    let fenced = ever_sealed;
    let stances = stances_of(&ledgers);

    // Stage 1: terminal reports, made through each voter's ledger, so
    // only a voter whose own seal is durable reports at all. Then a
    // replacement coordinator asks the same voters for a second root:
    // the two-root script. Whatever it obtains is kept apart, and the
    // sweep checks that no majority of everything reported certifies a
    // root other than the first.
    let mut reports = Vec::new();
    let mut late_reports = Vec::new();
    let mut terminal_refusals = Vec::new();
    if progress >= 1 {
        for (attempt, terminal_root) in [root(0x77), root(0x88)].into_iter().enumerate() {
            for ledger in &mut ledgers {
                match ledger.report_terminal(transition(THIS), terminal_root) {
                    Ok(report) if attempt == 0 => reports.push(report),
                    Ok(report) => late_reports.push(report),
                    Err(TerminalReportError::NotSealed) => {}
                    Err(e) => terminal_refusals.push(e),
                }
            }
        }
    }
    // Stage 2: the terminal certificate, if the seal certifies and the
    // reports are a majority.
    let mine: Vec<StanceRecord> = stances
        .iter()
        .filter(|s| s.transition == transition(THIS))
        .copied()
        .collect();
    let terminal = if progress >= 2 {
        seal(&old, transition(THIS), &mine)
            .ok()
            .and_then(|sealed| select_terminal(&old, &sealed, new.voters(), &reports).ok())
    } else {
        None
    };
    // Stage 3: installs and activation.
    let mut installs = Vec::new();
    let mut activation = None;
    if progress >= 3
        && let Some(certificate) = &terminal
    {
        for replica in new.voters() {
            installs.push(InstallRecord {
                replica: *replica,
                transition: transition(THIS),
                terminal_root: certificate.terminal_root(),
            });
        }
        if progress >= 4 {
            activation = activate(&new, certificate, &installs).ok();
        }
    }
    World {
        history,
        stances,
        refusals,
        fenced,
        reports,
        late_reports,
        terminal_refusals,
        terminal,
        installs,
        activation,
    }
}

impl World {
    fn evidence(&self, authorized: bool) -> Evidence<'_> {
        Evidence {
            authorized,
            stances: &self.stances,
            reports: &self.reports,
            terminal: self.terminal.as_ref(),
            installs: &self.installs,
            activation: self.activation.as_ref(),
        }
    }
}

#[derive(Serialize)]
struct Summary {
    worlds: usize,
    /// How many worlds resumed at each stage.
    stages: BTreeMap<String, usize>,
    /// Worlds in which a fence existed and `resume` said `Stable`. Must
    /// be zero.
    fence_cleared: usize,
    /// Worlds in which a seal and a cancellation both certified. Must
    /// be zero.
    sealed_and_cancelled: usize,
    /// Distinct refusals the stance ledgers produced.
    refusals: BTreeSet<String>,
}

#[test]
fn every_world_of_the_handoff_resumes_from_evidence_and_never_clears_a_fence() {
    let old = old_voters();
    let new = successor();
    let mut stages: BTreeMap<String, usize> = BTreeMap::new();
    let mut fence_cleared = 0;
    let mut sealed_and_cancelled = 0;
    let mut refusals = BTreeSet::new();
    let all = worlds();
    for (assignment, progress) in &all {
        let world = run(assignment, *progress);
        for e in &world.refusals {
            refusals.insert(format!("{e:?}"));
        }
        for e in &world.terminal_refusals {
            refusals.insert(format!("{e:?}"));
        }
        let stage = resume(&old, &new, transition(THIS), world.evidence(true));
        match stage {
            Ok(stage) => {
                *stages.entry(format!("{stage:?}")).or_default() += 1;
                if stage == Stage::Stable && !world.fenced.is_empty() {
                    fence_cleared += 1;
                }
            }
            Err(HandoffError::SealedAndCancelled) => sealed_and_cancelled += 1,
            // A fence another transition left is not this transition's
            // to resume. Counted as a stage so the fixture shows how
            // often the worlds reach it.
            Err(HandoffError::FencedByAnother { .. }) => {
                *stages.entry("FencedByAnother".into()).or_default() += 1;
            }
            Err(e) => panic!("world {assignment:?}/{progress} could not resume: {e:?}"),
        }

        // No voter ever recorded both stances for one transition.
        // This is what stops a seal and a cancellation both
        // certifying, and it has to hold over everything that was ever
        // durable: a certificate formed from an earlier row does not
        // stop existing when the row changes.
        let mut recorded: BTreeMap<(ReplicaId, Transition), BTreeSet<Stance>> = BTreeMap::new();
        for record in &world.history {
            recorded
                .entry((record.voter, record.transition))
                .or_default()
                .insert(record.stance);
        }
        for ((voter, _), stances) in &recorded {
            assert_eq!(
                stances.len(),
                1,
                "{voter:?} recorded both stances for one transition: {stances:?}"
            );
        }

        // A voter that ever sealed is still fenced in the evidence:
        // its row says so, for whichever transition it sealed.
        for voter in &world.fenced {
            assert!(
                world
                    .stances
                    .iter()
                    .any(|s| &s.voter == voter && s.stance == Stance::Sealed),
                "a fence vanished from the evidence: {voter:?}"
            );
        }

        // Resume is a function of the evidence, not of anything else:
        // asking twice gives the same answer, and asking with the
        // transition unauthorized can only make the answer weaker when
        // there is nothing else to go on.
        assert_eq!(
            resume(&old, &new, transition(THIS), world.evidence(true)).ok(),
            resume(&old, &new, transition(THIS), world.evidence(true)).ok()
        );
        let unauthorized = resume(&old, &new, transition(THIS), world.evidence(false)).ok();
        if let Ok(stage) = stage {
            if stage != Stage::Preparing {
                assert_eq!(
                    unauthorized,
                    Some(stage),
                    "an authorization flag changed a stage that records justify"
                );
            } else {
                assert_eq!(unauthorized, Some(Stage::Stable));
            }
        }

        // One terminal state: no voter reported two roots, and no
        // majority of everything any coordinator obtained certifies a
        // second one. Without the once-only report the replacement's
        // requests would succeed and a majority of them would select
        // root 88 beside the first coordinator's 77.
        assert!(
            world.late_reports.is_empty(),
            "a voter reported a second terminal root: {:?}",
            world.late_reports
        );
        let every_report: Vec<TerminalReport> = world
            .reports
            .iter()
            .chain(&world.late_reports)
            .copied()
            .collect();
        let mut roots_by_voter: BTreeMap<ReplicaId, BTreeSet<Digest32>> = BTreeMap::new();
        for report in &every_report {
            roots_by_voter
                .entry(report.voter)
                .or_default()
                .insert(report.terminal_root);
        }
        assert!(roots_by_voter.values().all(|roots| roots.len() == 1));
        let mine: Vec<StanceRecord> = world
            .stances
            .iter()
            .filter(|s| s.transition == transition(THIS))
            .copied()
            .collect();
        if let Ok(sealed) = seal(&old, transition(THIS), &mine) {
            let mut selected: BTreeSet<Digest32> = BTreeSet::new();
            for subset in subsets(&every_report) {
                if let Ok(certificate) = select_terminal(&old, &sealed, new.voters(), &subset) {
                    selected.insert(certificate.terminal_root());
                }
            }
            assert!(
                selected.len() <= 1,
                "two terminal roots were certified for one transition: {selected:?}"
            );
        }

        // A certificate names the successor its caller passed -- the
        // module cannot check it against the transition's subject, so
        // this is the caller's obligation rather than a property -- and
        // it exists only after a seal.
        if let Some(certificate) = &world.terminal {
            assert_eq!(certificate.successor(), new.voters());
            assert_eq!(certificate.transition(), transition(THIS));
            let mine: Vec<StanceRecord> = world
                .stances
                .iter()
                .filter(|s| s.transition == transition(THIS))
                .copied()
                .collect();
            assert!(
                seal(&old, transition(THIS), &mine).is_ok(),
                "a terminal certificate was selected without a seal"
            );
        }

        // An activation implies a majority of the successor installed
        // the certificate's exact root.
        if let Some(activation) = &world.activation {
            assert!(activation.installers().len() >= new.majority());
            let certificate = world.terminal.as_ref().expect("activation implies one");
            assert_eq!(activation.terminal_root(), certificate.terminal_root());
        }
    }
    assert_eq!(fence_cleared, 0, "a fence was cleared by a resume");
    assert_eq!(
        sealed_and_cancelled, 0,
        "a seal and a cancellation both certified"
    );
    fixture(
        "handoff_scenarios.json",
        &Summary {
            worlds: all.len(),
            stages,
            fence_cleared,
            sealed_and_cancelled,
            refusals,
        },
    );
}

/// Every subset of `items`.
fn subsets<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    let mut out: Vec<Vec<T>> = vec![Vec::new()];
    for item in items {
        let mut next = out.clone();
        for mut subset in out {
            subset.push(item.clone());
            next.push(subset);
        }
        out = next;
    }
    out
}

/// Evidence only accumulates, and the stage it justifies only moves
/// forward.
#[test]
fn accumulating_evidence_never_moves_the_stage_backwards() {
    let old = old_voters();
    let new = successor();
    let order = [
        Stage::Stable,
        Stage::Preparing,
        Stage::Sealing,
        Stage::TerminalRecovery,
        Stage::Installing,
        Stage::Activating,
        Stage::Served,
    ];
    let rank = |stage: Stage| order.iter().position(|s| *s == stage).unwrap();

    // One old voter seals at a time, then the coordinator makes each
    // later stage durable. Every step is a superset of the one before.
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    let mut seen = rank(Stage::Stable);
    let mut stances = Vec::new();
    let mut reports: Vec<TerminalReport> = Vec::new();
    let mut terminal: Option<TerminalCertificate> = None;
    let mut installs: Vec<InstallRecord> = Vec::new();
    let mut activation = None;

    let observe = |stage: Stage, seen: &mut usize| {
        assert!(
            rank(stage) >= *seen,
            "the stage went backwards to {stage:?} from rank {seen}"
        );
        *seen = rank(stage);
    };

    observe(
        resume(
            &old,
            &new,
            transition(THIS),
            Evidence {
                authorized: true,
                stances: &stances,
                reports: &reports,
                terminal: terminal.as_ref(),
                installs: &installs,
                activation: activation.as_ref(),
            },
        )
        .unwrap(),
        &mut seen,
    );

    for i in 0..3u8 {
        stances.push(
            ledgers[i as usize]
                .record(&old, transition(THIS), Stance::Sealed)
                .unwrap(),
        );
        reports.push(
            ledgers[i as usize]
                .report_terminal(transition(THIS), root(0x77))
                .unwrap(),
        );
        observe(
            resume(
                &old,
                &new,
                transition(THIS),
                Evidence {
                    authorized: true,
                    stances: &stances,
                    reports: &reports,
                    terminal: terminal.as_ref(),
                    installs: &installs,
                    activation: activation.as_ref(),
                },
            )
            .unwrap(),
            &mut seen,
        );
    }

    let sealed = seal(&old, transition(THIS), &stances).unwrap();
    terminal = Some(select_terminal(&old, &sealed, new.voters(), &reports).unwrap());
    observe(
        resume(
            &old,
            &new,
            transition(THIS),
            Evidence {
                authorized: true,
                stances: &stances,
                reports: &reports,
                terminal: terminal.as_ref(),
                installs: &installs,
                activation: activation.as_ref(),
            },
        )
        .unwrap(),
        &mut seen,
    );

    let certificate = terminal.clone().unwrap();
    for replica in new.voters() {
        installs.push(InstallRecord {
            replica: *replica,
            transition: transition(THIS),
            terminal_root: certificate.terminal_root(),
        });
        observe(
            resume(
                &old,
                &new,
                transition(THIS),
                Evidence {
                    authorized: true,
                    stances: &stances,
                    reports: &reports,
                    terminal: terminal.as_ref(),
                    installs: &installs,
                    activation: activation.as_ref(),
                },
            )
            .unwrap(),
            &mut seen,
        );
    }

    activation = Some(activate(&new, &certificate, &installs).unwrap());
    let served = resume(
        &old,
        &new,
        transition(THIS),
        Evidence {
            authorized: true,
            stances: &stances,
            reports: &reports,
            terminal: terminal.as_ref(),
            installs: &installs,
            activation: activation.as_ref(),
        },
    )
    .unwrap();
    observe(served, &mut seen);
    assert_eq!(served, Stage::Served);
    assert_eq!(seen, rank(Stage::Served));
}

#[derive(Serialize)]
struct Counterexample {
    name: &'static str,
    rule_removed: &'static str,
    witness: String,
    refused: String,
}

/// The rules removed one at a time, each with the counterexample it
/// exists for.
#[test]
fn removing_a_handoff_rule_produces_the_counterexample_it_exists_for() {
    let old = old_voters();
    let new = successor();
    let mut out = Vec::new();

    // 1. A voter may reverse its stance.
    //
    // Two voters seal, so the old configuration is fenced. If either
    // could then cancel, two voters would have cancelled and a
    // cancellation would certify -- a fence cleared by a retry, and the
    // old configuration serving again after an irreversible seal.
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    ledgers[0]
        .record(&old, transition(THIS), Stance::Sealed)
        .unwrap();
    ledgers[1]
        .record(&old, transition(THIS), Stance::Sealed)
        .unwrap();
    ledgers[2]
        .record(&old, transition(THIS), Stance::Cancelled)
        .unwrap();
    let refused = ledgers[0]
        .record(&old, transition(THIS), Stance::Cancelled)
        .expect_err("a sealed voter cannot cancel");
    assert_eq!(
        refused,
        StanceError::Reversal {
            held: Stance::Sealed
        }
    );
    let forged: Vec<StanceRecord> = [r(0), r(2)]
        .iter()
        .map(|voter| StanceRecord {
            voter: *voter,
            transition: transition(THIS),
            stance: Stance::Cancelled,
        })
        .collect();
    let both = cancel(&old, transition(THIS), &forged);
    assert!(both.is_ok(), "the forged majority cancels");
    let stances = stances_of(&ledgers);
    assert_eq!(
        resume(
            &old,
            &new,
            transition(THIS),
            Evidence {
                authorized: true,
                stances: &stances,
                reports: &[],
                terminal: None,
                installs: &[],
                activation: None,
            }
        )
        .unwrap(),
        Stage::TerminalRecovery
    );
    out.push(Counterexample {
        name: "a_voter_may_reverse_its_stance",
        rule_removed: "StanceLedger refuses the opposite stance for a transition it decided",
        witness: "voters 0 and 2 cancel a transition voters 0 and 1 already sealed: a fence \
                  cleared by a retry, and the old configuration serving again"
            .into(),
        refused: format!("{refused:?}"),
    });

    // 2. A terminal certificate may be selected without a seal.
    //
    // Two unsealed old voters report the same root. Before the fence an
    // old voter can still accept work, so what it calls terminal is not
    // terminal: the certificate would bind a state the old
    // configuration could still move past.
    let unsealed_reports: Vec<TerminalReport> = [r(0), r(1)]
        .iter()
        .map(|voter| TerminalReport {
            voter: *voter,
            transition: transition(THIS),
            terminal_root: root(0x77),
        })
        .collect();
    let no_stances: Vec<StanceRecord> = Vec::new();
    let no_seal = seal(&old, transition(THIS), &no_stances).expect_err("nothing sealed");
    assert_eq!(no_seal, HandoffError::NoQuorum { have: 0, need: 2 });
    // With a seal in hand the same reports do certify, which is the
    // point: the reports are not the problem, the missing fence is.
    let sealed = seal(
        &old,
        transition(THIS),
        &[
            StanceRecord {
                voter: r(0),
                transition: transition(THIS),
                stance: Stance::Sealed,
            },
            StanceRecord {
                voter: r(1),
                transition: transition(THIS),
                stance: Stance::Sealed,
            },
        ],
    )
    .unwrap();
    let certificate =
        select_terminal(&old, &sealed, new.voters(), &unsealed_reports).expect("sealed");
    out.push(Counterexample {
        name: "terminal_state_without_a_seal",
        rule_removed: "select_terminal takes a SealCertificate, so there is no selection before \
                       the fence",
        witness: format!(
            "two unsealed voters report root {:02x} as terminal while the old configuration can \
             still accept work",
            certificate.terminal_root().0[0]
        ),
        refused: format!("{no_seal:?}"),
    });

    // 3. Mixed terminal roots may be merged.
    //
    // Two old voters report different roots. Merging them, or taking
    // the first, is a history nobody agreed on.
    let mixed = vec![
        TerminalReport {
            voter: r(0),
            transition: transition(THIS),
            terminal_root: root(0x77),
        },
        TerminalReport {
            voter: r(1),
            transition: transition(THIS),
            terminal_root: root(0x88),
        },
    ];
    let refused_mixed =
        select_terminal(&old, &sealed, new.voters(), &mixed).expect_err("mixed roots");
    assert_eq!(refused_mixed, HandoffError::MixedTerminal);
    out.push(Counterexample {
        name: "mixed_terminal_roots_merged",
        rule_removed: "select_terminal refuses reports that disagree about the terminal state",
        witness: "voters 0 and 1 report roots 77 and 88 for one transition; taking either would \
                  bind a terminal state the other never had"
            .into(),
        refused: format!("{refused_mixed:?}"),
    });

    // 4. A minority of the successor may activate.
    //
    // One new replica installs. Activating on it would let a successor
    // serve while most of it holds nothing, and a later majority of the
    // successor would have no copy of the terminal state to recover
    // from.
    let one_install = vec![InstallRecord {
        replica: r(3),
        transition: transition(THIS),
        terminal_root: certificate.terminal_root(),
    }];
    let refused_minority =
        activate(&new, &certificate, &one_install).expect_err("a minority activates nothing");
    assert_eq!(
        refused_minority,
        HandoffError::NoQuorum { have: 1, need: 2 }
    );
    out.push(Counterexample {
        name: "a_minority_of_the_successor_activates",
        rule_removed: "activate requires a majority of the successor to have installed the root",
        witness: "replica 3 alone installs and the successor serves; a later majority of it holds \
                  no copy of the terminal state"
            .into(),
        refused: format!("{refused_minority:?}"),
    });

    // 5. An installation of another root counts.
    let wrong_root: Vec<InstallRecord> = new
        .voters()
        .iter()
        .map(|replica| InstallRecord {
            replica: *replica,
            transition: transition(THIS),
            terminal_root: root(0x99),
        })
        .collect();
    let refused_root =
        activate(&new, &certificate, &wrong_root).expect_err("another history is not an install");
    assert_eq!(refused_root, HandoffError::WrongTerminalRoot);
    out.push(Counterexample {
        name: "an_installation_of_another_root_counts",
        rule_removed: "activate refuses an installation naming a root other than the \
                       certificate's",
        witness: "the whole successor installs root 99 and activates under a certificate binding \
                  another state"
            .into(),
        refused: format!("{refused_root:?}"),
    });

    // 6. A replaced cancellation may be overwritten.
    //
    // Voters 0 and 1 cancel this transition, then cancel another that
    // replaced it, then receive a delayed seal for this one. A ledger
    // that forgot the first cancellation would record both stances for
    // it: the cancellation certified earlier and the seal certifying
    // now, with the replacement fenced behind the resurrected one.
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    let mut first = Vec::new();
    for ledger in &mut ledgers[..2] {
        first.push(
            ledger
                .record(&old, transition(THIS), Stance::Cancelled)
                .unwrap(),
        );
        ledger
            .record(&old, transition(OTHER), Stance::Cancelled)
            .unwrap();
    }
    let certified = cancel(&old, transition(THIS), &first).expect("a majority cancelled");
    let refused_late = ledgers[0]
        .record(&old, transition(THIS), Stance::Sealed)
        .expect_err("a cancelled transition stays cancelled");
    assert_eq!(
        refused_late,
        StanceError::Reversal {
            held: Stance::Cancelled
        }
    );
    out.push(Counterexample {
        name: "a_replaced_cancellation_is_forgotten",
        rule_removed: "StanceLedger keeps every cancelled transition as a tombstone",
        witness: format!(
            "voters {:?} cancel a1, cancel b2, then seal a1: a1 is both cancelled and sealed, and              b2 is fenced behind it",
            certified.signers().iter().map(|v| v.0[0]).collect::<Vec<_>>()
        ),
        refused: format!("{refused_late:?}"),
    });

    // 7. A sealed voter may report a second terminal root.
    //
    // One coordinator reads voters 0 and 1 and selects root 77; its
    // replacement reads 1 and 2 and asks for root 88. If voter 1 could
    // answer again, both majorities would certify and the transition
    // would have two terminal states.
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    let stances: Vec<StanceRecord> = ledgers
        .iter_mut()
        .map(|l| l.record(&old, transition(THIS), Stance::Sealed).unwrap())
        .collect();
    let sealed = seal(&old, transition(THIS), &stances).unwrap();
    let first: Vec<TerminalReport> = ledgers[..2]
        .iter_mut()
        .map(|l| l.report_terminal(transition(THIS), root(0x77)).unwrap())
        .collect();
    let chosen = select_terminal(&old, &sealed, new.voters(), &first).unwrap();
    let refused_second = ledgers[1]
        .report_terminal(transition(THIS), root(0x88))
        .expect_err("one report per voter");
    assert_eq!(
        refused_second,
        TerminalReportError::Conflicting { held: root(0x77) }
    );
    let second = vec![
        ledgers[2]
            .report_terminal(transition(THIS), root(0x88))
            .unwrap(),
    ];
    assert_eq!(
        select_terminal(&old, &sealed, new.voters(), &second),
        Err(HandoffError::NoQuorum { have: 1, need: 2 })
    );
    out.push(Counterexample {
        name: "a_voter_reports_two_terminal_roots",
        rule_removed: "StanceLedger::report_terminal records one root per sealed voter",
        witness: format!(
            "voters 0 and 1 report root {:02x}, then voters 1 and 2 report root 88: two terminal              certificates for one transition, with voter 1 in both",
            chosen.terminal_root().0[0]
        ),
        refused: format!("{refused_second:?}"),
    });

    fixture("handoff_counterexamples.json", &out);
}

/// Every certificate is a majority of the configuration the transition
/// names, never of whatever set the caller offered.
#[test]
fn a_certificate_counts_only_the_configuration_the_transition_names() {
    let old = old_voters();
    let new = successor();
    let sealed_by: Vec<StanceRecord> = [r(0), r(1)]
        .iter()
        .map(|voter| StanceRecord {
            voter: *voter,
            transition: transition(THIS),
            stance: Stance::Sealed,
        })
        .collect();

    // A stale configuration holding the same replica ids is not the one
    // being left, so its majority fences nothing.
    let stale = EpochVoters::new(epoch(OLD - 1), [r(0), r(1)].into()).unwrap();
    assert_eq!(
        seal(&stale, transition(THIS), &sealed_by),
        Err(HandoffError::EpochMismatch)
    );
    assert_eq!(
        cancel(&stale, transition(THIS), &[]),
        Err(HandoffError::EpochMismatch)
    );
    let certificate = seal(&old, transition(THIS), &sealed_by).unwrap();
    let reports: Vec<TerminalReport> = [r(0), r(1)]
        .iter()
        .map(|voter| TerminalReport {
            voter: *voter,
            transition: transition(THIS),
            terminal_root: root(0x77),
        })
        .collect();
    assert_eq!(
        select_terminal(&stale, &certificate, new.voters(), &reports),
        Err(HandoffError::EpochMismatch)
    );
    let terminal = select_terminal(&old, &certificate, new.voters(), &reports).unwrap();

    // The successor that installs is the one the certificate names.
    // Another epoch, or another set of replicas under the right epoch,
    // is a set the old majority never selected, however many of it
    // hold the root.
    let installs = |voters: &EpochVoters| -> Vec<InstallRecord> {
        voters
            .voters()
            .iter()
            .map(|replica| InstallRecord {
                replica: *replica,
                transition: transition(THIS),
                terminal_root: terminal.terminal_root(),
            })
            .collect()
    };
    let later = EpochVoters::new(epoch(NEW + 1), new.voters().clone()).unwrap();
    assert_eq!(
        activate(&later, &terminal, &installs(&later)),
        Err(HandoffError::EpochMismatch)
    );
    let strangers = EpochVoters::new(epoch(NEW), [r(5), r(6), r(7)].into()).unwrap();
    assert_eq!(
        activate(&strangers, &terminal, &installs(&strangers)),
        Err(HandoffError::WrongSuccessor)
    );
    assert!(activate(&new, &terminal, &installs(&new)).is_ok());

    // And a recovering coordinator holding the wrong configurations
    // has no evidence to read them against.
    let evidence = Evidence {
        authorized: true,
        stances: &sealed_by,
        reports: &reports,
        terminal: Some(&terminal),
        installs: &[],
        activation: None,
    };
    assert_eq!(
        resume(&stale, &new, transition(THIS), evidence),
        Err(HandoffError::EpochMismatch)
    );
    assert_eq!(
        resume(&old, &later, transition(THIS), evidence),
        Err(HandoffError::EpochMismatch)
    );
    assert_eq!(
        resume(&old, &new, transition(THIS), evidence),
        Ok(Stage::Installing)
    );
}

/// A cancellation is a pre-seal outcome: one voter's fence refuses it
/// however many of the others cancelled.
#[test]
fn a_single_fence_refuses_a_cancellation() {
    let old = old_voters();
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    ledgers[0]
        .record(&old, transition(THIS), Stance::Sealed)
        .unwrap();
    ledgers[1]
        .record(&old, transition(THIS), Stance::Cancelled)
        .unwrap();
    ledgers[2]
        .record(&old, transition(THIS), Stance::Cancelled)
        .unwrap();
    let stances = stances_of(&ledgers);
    assert_eq!(
        cancel(&old, transition(THIS), &stances),
        Err(HandoffError::Fenced { by: r(0) })
    );
    // A fence left by another transition is not this one's business
    // here; `resume` reports it as such.
    let elsewhere = [StanceRecord {
        voter: r(0),
        transition: transition(OTHER),
        stance: Stance::Sealed,
    }];
    assert_eq!(
        cancel(&old, transition(THIS), &elsewhere),
        Err(HandoffError::WrongTransition)
    );
    // The cancelled majority does not send the configuration back to
    // `Stable`: the sealed voter continues, or blocks.
    assert_eq!(
        resume(
            &old,
            &successor(),
            transition(THIS),
            Evidence {
                authorized: true,
                stances: &stances,
                reports: &[],
                terminal: None,
                installs: &[],
                activation: None,
            }
        ),
        Ok(Stage::Sealing)
    );
}

/// A sealed voter is not available for another transition, and a stale
/// attempt is a different transition rather than an older version of
/// this one.
#[test]
fn a_sealed_voter_finishes_the_transition_it_sealed() {
    let old = old_voters();
    let mut ledger = StanceLedger::new(r(0), epoch(OLD));
    ledger
        .record(&old, transition(THIS), Stance::Sealed)
        .unwrap();
    assert_eq!(
        ledger.record(&old, transition(OTHER), Stance::Sealed),
        Err(StanceError::Sealed {
            held: transition(THIS)
        })
    );
    assert!(ledger.fenced());

    // A cancelled transition releases the domain: another one may
    // start. The cancellation is kept, so a late seal for it is still a
    // reversal and a repeated refusal is still the same refusal.
    let mut released = StanceLedger::new(r(1), epoch(OLD));
    released
        .record(&old, transition(THIS), Stance::Cancelled)
        .unwrap();
    released
        .record(&old, transition(OTHER), Stance::Sealed)
        .expect("a cancelled transition frees the domain");
    assert!(released.fenced());
    assert_eq!(released.cancelled(), &[transition(THIS)].into());
    assert_eq!(
        released.record(&old, transition(THIS), Stance::Cancelled),
        Ok(StanceRecord {
            voter: r(1),
            transition: transition(THIS),
            stance: Stance::Cancelled,
        })
    );
    assert_eq!(
        released.record(&old, transition(THIS), Stance::Sealed),
        Err(StanceError::Reversal {
            held: Stance::Cancelled
        })
    );
    assert_eq!(
        stances_of(std::slice::from_ref(&released)),
        vec![
            StanceRecord {
                voter: r(1),
                transition: transition(THIS),
                stance: Stance::Cancelled,
            },
            StanceRecord {
                voter: r(1),
                transition: transition(OTHER),
                stance: Stance::Sealed,
            },
        ]
    );
    // The row survives recovery with its tombstones and its report.
    let report = released
        .report_terminal(transition(OTHER), root(0x77))
        .unwrap();
    let recovered = StanceLedger::recovered(
        r(1),
        epoch(OLD),
        released.held(),
        released.cancelled().clone(),
        released.terminal(),
    );
    assert_eq!(recovered, released);
    assert_eq!(recovered.terminal(), Some(report));

    // Evidence about another transition is not evidence about this one.
    let stray = [StanceRecord {
        voter: r(0),
        transition: transition(OTHER),
        stance: Stance::Sealed,
    }];
    assert_eq!(
        seal(&old, transition(THIS), &stray),
        Err(HandoffError::WrongTransition)
    );

    // And an observer seals nothing.
    let mut observer = StanceLedger::new(r(9), epoch(OLD));
    assert_eq!(
        observer.record(&old, transition(THIS), Stance::Sealed),
        Err(StanceError::NotAVoter)
    );
    assert_eq!(
        seal(
            &old,
            transition(THIS),
            &[StanceRecord {
                voter: r(9),
                transition: transition(THIS),
                stance: Stance::Sealed
            }]
        ),
        Err(HandoffError::NotAVoter { replica: r(9) })
    );
}

/// The reviewer-shaped sequence end to end: voters 0 and 1 cancel this
/// transition, then cancel its replacement, then receive a delayed seal
/// for the first. The seal is refused, so no seal certifies beside the
/// cancellation and the replacement is not fenced behind a resurrected
/// transition.
#[test]
fn a_cancelled_transition_is_not_resurrected_by_a_late_seal() {
    let old = old_voters();
    let new = successor();
    let mut ledgers: Vec<StanceLedger> = (0..3)
        .map(|i| StanceLedger::new(r(i), epoch(OLD)))
        .collect();
    for ledger in &mut ledgers[..2] {
        ledger
            .record(&old, transition(THIS), Stance::Cancelled)
            .unwrap();
        ledger
            .record(&old, transition(OTHER), Stance::Cancelled)
            .unwrap();
        assert_eq!(
            ledger.record(&old, transition(THIS), Stance::Sealed),
            Err(StanceError::Reversal {
                held: Stance::Cancelled
            })
        );
        assert!(!ledger.fenced());
    }
    let stances = stances_of(&ledgers);
    assert!(cancel(&old, transition(THIS), &mine(&stances, THIS)).is_ok());
    assert!(seal(&old, transition(THIS), &mine(&stances, THIS)).is_err());
    let evidence = |stances| Evidence {
        authorized: true,
        stances,
        reports: &[],
        terminal: None,
        installs: &[],
        activation: None,
    };
    assert_eq!(
        resume(&old, &new, transition(THIS), evidence(&stances)),
        Ok(Stage::Stable)
    );
    assert_eq!(
        resume(&old, &new, transition(OTHER), evidence(&stances)),
        Ok(Stage::Stable)
    );
}

fn mine(stances: &[StanceRecord], subject: u8) -> Vec<StanceRecord> {
    stances
        .iter()
        .filter(|s| s.transition == transition(subject))
        .copied()
        .collect()
}

/// Terminal reports are one per voter and only after its own seal.
#[test]
fn a_terminal_report_is_made_once_and_only_after_the_voters_seal() {
    let old = old_voters();
    let mut ledger = StanceLedger::new(r(0), epoch(OLD));
    assert_eq!(
        ledger.report_terminal(transition(THIS), root(0x77)),
        Err(TerminalReportError::NotSealed)
    );
    ledger
        .record(&old, transition(THIS), Stance::Sealed)
        .unwrap();
    assert_eq!(
        ledger.report_terminal(transition(OTHER), root(0x77)),
        Err(TerminalReportError::NotSealed)
    );
    let report = ledger
        .report_terminal(transition(THIS), root(0x77))
        .unwrap();
    // A retry after a lost reply is the same report.
    assert_eq!(
        ledger.report_terminal(transition(THIS), root(0x77)),
        Ok(report)
    );
    assert_eq!(
        ledger.report_terminal(transition(THIS), root(0x88)),
        Err(TerminalReportError::Conflicting { held: root(0x77) })
    );
    assert_eq!(ledger.terminal(), Some(report));

    // A cancelling voter reports nothing.
    let mut refusing = StanceLedger::new(r(1), epoch(OLD));
    refusing
        .record(&old, transition(THIS), Stance::Cancelled)
        .unwrap();
    assert_eq!(
        refusing.report_terminal(transition(THIS), root(0x77)),
        Err(TerminalReportError::NotSealed)
    );
}
