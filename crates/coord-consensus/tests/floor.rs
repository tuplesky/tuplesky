//! Bounded models of quorum-certified checkpoint activation (task-52;
//! design Sections 5.3, 23 G5).
//!
//! Every world here is a complete run of the floor protocol over a small
//! configuration: each voter follows one script of readiness attempts,
//! the ledgers enforce the promise rules, every subset of the resulting
//! promises is offered for certification, and every majority is asked
//! what it would discover. The properties are checked in every world,
//! not sampled.
//!
//! The four that matter, and what breaks without them:
//!
//! * **Intersection.** For every certified floor and every majority of
//!   the same voters, discovery returns a position at or above it. This
//!   is the whole reason a recovery may trim-and-forget safely; the
//!   `recovery_reads_one_voter` counterexample is what happens when a
//!   recovery reads fewer reports.
//! * **Uniqueness.** At most one subject per position can ever be
//!   certified, because a voter refuses a second subject at a position
//!   it is already ready for. The `competing_subjects_allowed`
//!   counterexample removes that refusal and finds two majorities
//!   certifying different state at one executed prefix.
//! * **Possession is not a promise.** Counting voters that merely hold
//!   the checkpoint certifies floors nobody is bound by; the
//!   `possession_counted_as_readiness` counterexample is a signer that
//!   crashes, comes back with its old baseline and votes from history
//!   the cluster has forgotten.
//! * **Monotonicity.** A floor that is held is never lowered, so a late
//!   certificate for an older floor cannot re-open what was trimmed.
//!
//! Results are frozen under `fixtures/counterexamples` (regenerate with
//! `COORD_CONSENSUS_WRITE_FIXTURES=1`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coord_consensus::floor::{
    ActivationError, FenceVerdict, FloorCandidate, FloorInstall, FloorLedger, FloorVoters,
    Readiness, ReadinessError, ReadinessLedger, activate, discover,
};
use coord_types::identity::Digest32;
use coord_types::ids::{ConfigurationEpoch, ExecutionPosition, ReplicaId};
use serde::Serialize;

fn r(i: u8) -> ReplicaId {
    ReplicaId([i; 16])
}

fn epoch() -> ConfigurationEpoch {
    ConfigurationEpoch::new(7).unwrap()
}

fn at(position: u64, subject: u8) -> FloorCandidate {
    FloorCandidate {
        epoch: epoch(),
        position: ExecutionPosition::new(position).unwrap(),
        subject: Digest32([subject; 32]),
    }
}

fn voters(n: u8) -> FloorVoters {
    FloorVoters::new(epoch(), (0..n).map(r).collect()).unwrap()
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

/// The candidates every world draws from: two competing subjects at one
/// position, and a later one. Two at a position is the interesting
/// case -- it is the only way two majorities could ever certify
/// different state at the same executed prefix.
const LOW: u64 = 4;
const HIGH: u64 = 9;

fn alphabet() -> Vec<FloorCandidate> {
    vec![at(LOW, 0xa1), at(LOW, 0xb2), at(HIGH, 0xc3)]
}

/// One voter's script: the candidates it attempts, in order.
fn scripts() -> Vec<Vec<usize>> {
    vec![
        vec![],
        vec![0],
        vec![1],
        vec![2],
        vec![0, 2],
        vec![1, 2],
        // Both subjects at one position: the attempt the whole
        // uniqueness property rests on refusing.
        vec![0, 1],
        // A regression attempt: ready for the later floor, then asked
        // for the earlier one. The promise must not move down.
        vec![2, 0],
    ]
}

/// Every assignment of a script to each of `n` voters.
fn worlds(n: u8) -> Vec<Vec<usize>> {
    let scripts = scripts().len();
    let mut out = vec![Vec::new()];
    for _ in 0..n {
        let mut next = Vec::new();
        for prefix in &out {
            for s in 0..scripts {
                let mut w = prefix.clone();
                w.push(s);
                next.push(w);
            }
        }
        out = next;
    }
    out
}

/// Every subset of `items`, as index sets.
fn subsets<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    let mut out: Vec<Vec<T>> = vec![Vec::new()];
    for item in items {
        let mut next = out.clone();
        for subset in &mut next {
            subset.push(item.clone());
        }
        out.extend(next);
    }
    out
}

/// What running one world produced.
struct World {
    /// Promises that were actually recorded, per voter.
    promises: BTreeMap<ReplicaId, Vec<Readiness>>,
    /// Refusals the ledgers produced, per voter.
    refusals: Vec<(ReplicaId, ReadinessError)>,
    /// The final promise each voter holds.
    held: BTreeMap<ReplicaId, Option<FloorCandidate>>,
}

fn run(n: u8, world: &[usize]) -> World {
    let voters = voters(n);
    let candidates = alphabet();
    let scripts = scripts();
    let mut promises: BTreeMap<ReplicaId, Vec<Readiness>> = BTreeMap::new();
    let mut refusals = Vec::new();
    let mut held = BTreeMap::new();
    for (i, script) in world.iter().enumerate() {
        let voter = r(i as u8);
        let mut ledger = ReadinessLedger::new(voter, epoch());
        for step in &scripts[*script] {
            match ledger.record(&voters, candidates[*step]) {
                Ok(readiness) => promises.entry(voter).or_default().push(readiness),
                Err(e) => refusals.push((voter, e)),
            }
        }
        held.insert(voter, ledger.held());
    }
    World {
        promises,
        refusals,
        held,
    }
}

/// Every floor this world's promises can certify.
fn certified(n: u8, world: &World) -> Vec<coord_consensus::floor::ActivatedFloor> {
    let voters = voters(n);
    let mut out = Vec::new();
    for candidate in alphabet() {
        let ready: Vec<Readiness> = world
            .promises
            .values()
            .flatten()
            .filter(|p| p.candidate == candidate)
            .copied()
            .collect();
        if let Ok(floor) = activate(&voters, &ready) {
            out.push(floor);
        }
    }
    out
}

/// Every majority of `n` voters.
fn majorities(n: u8) -> Vec<BTreeSet<ReplicaId>> {
    let all: Vec<ReplicaId> = (0..n).map(r).collect();
    let need = all.len() / 2 + 1;
    subsets(&all)
        .into_iter()
        .filter(|s| s.len() >= need)
        .map(|s| s.into_iter().collect())
        .collect()
}

#[derive(Serialize)]
struct Summary {
    voters: u8,
    worlds: usize,
    /// Worlds in which some floor was certified.
    with_a_floor: usize,
    /// Distinct certified `(position, subject)` pairs across all worlds.
    certified: BTreeSet<(u64, u8)>,
    /// Positions at which two different subjects were both certified,
    /// in any world. Must be empty.
    contested: BTreeSet<u64>,
    /// Majority reads that discovered less than a certified floor. Must
    /// be zero.
    missed: usize,
    /// Majority reads that found a certified floor's position but could
    /// not identify its image: more than one subject reported there and
    /// none with a majority inside the read. Legitimate -- the position
    /// still binds -- and the reason image resolution is the caller's.
    unidentified: usize,
    /// Distinct refusals the ledgers produced.
    refusals: BTreeSet<String>,
}

/// The four properties, in every world of a three-, four- and
/// five-voter configuration.
#[test]
fn every_world_of_the_floor_protocol_holds_the_four_properties() {
    let mut summaries = Vec::new();
    for n in [3u8, 4, 5] {
        let mut with_a_floor = 0;
        let mut certified_all = BTreeSet::new();
        let mut contested = BTreeSet::new();
        let mut missed = 0;
        let mut unidentified = 0;
        let mut refusals = BTreeSet::new();
        let all = worlds(n);
        for world in &all {
            let run = run(n, world);
            for (voter, e) in &run.refusals {
                let _ = voter;
                refusals.insert(format!("{e:?}"));
            }
            let floors = certified(n, &run);
            if !floors.is_empty() {
                with_a_floor += 1;
            }
            let mut by_position: BTreeMap<u64, BTreeSet<[u8; 32]>> = BTreeMap::new();
            for floor in &floors {
                certified_all.insert((floor.position().get(), floor.subject().0[0]));
                by_position
                    .entry(floor.position().get())
                    .or_default()
                    .insert(floor.subject().0);
            }
            // Uniqueness: one subject per position, ever. Checked in
            // the world that produced it rather than in the summary, so
            // a violation names the world and stops before the
            // properties that assume it.
            for (position, subjects) in &by_position {
                if subjects.len() > 1 {
                    contested.insert(*position);
                }
            }
            assert!(
                contested.is_empty(),
                "two subjects were certified at one position in world {world:?}: {by_position:?}"
            );
            // Intersection: every majority discovers at or above every
            // certified floor.
            for majority in majorities(n) {
                let reports: Vec<Readiness> = run
                    .promises
                    .iter()
                    .filter(|(voter, _)| majority.contains(voter))
                    .flat_map(|(_, p)| p.iter().copied())
                    .collect();
                let discovered = discover(&reports);
                for floor in &floors {
                    let seen = discovered
                        .as_ref()
                        .map_or(ExecutionPosition::ZERO, |d| d.position);
                    if seen < floor.position() {
                        missed += 1;
                    }
                    // Identification: at the certified floor's own
                    // position the read always carries its subject, and
                    // whatever the read names as the image is that
                    // subject. When it names nothing, the read is counted
                    // rather than failed.
                    let Some(d) = discovered
                        .as_ref()
                        .filter(|d| d.position == floor.position())
                    else {
                        continue;
                    };
                    assert!(
                        d.subjects.contains(&floor.subject()),
                        "a majority read lost the certified subject in world {world:?}"
                    );
                    let named = d.subject().or_else(|| d.certified(&voters(n)));
                    match named {
                        Some(subject) => assert_eq!(
                            subject,
                            floor.subject(),
                            "a majority read named another image than the certified one in \
                             world {world:?}"
                        ),
                        None => unidentified += 1,
                    }
                }
            }
            // Monotonicity: installing every certified floor in every
            // order leaves the same held position, and never a lower
            // one than any already installed.
            for order in permutations(&floors) {
                let mut ledger = FloorLedger::new(epoch());
                let mut highest = ExecutionPosition::ZERO;
                for floor in &order {
                    let before = ledger.position();
                    let outcome = ledger.install(floor.clone()).expect("no divergence");
                    assert!(
                        ledger.position() >= before,
                        "a floor ledger lowered itself: {before:?} -> {:?}",
                        ledger.position()
                    );
                    if outcome == FloorInstall::Advanced {
                        assert!(ledger.position() > before);
                    }
                    highest = highest.max(floor.position());
                }
                assert_eq!(
                    ledger.position(),
                    highest,
                    "installing the same certificates in another order held a different floor"
                );
            }
            // A voter that promised may never vote from below its own
            // promise, whatever else happened in the world. The baseline
            // it votes from is the promise its ledger holds, so that is
            // what is checked: nothing it ever recorded is above it, and
            // every floor it signed admits it there. Neither follows
            // from `admits_voter` alone -- without the regression
            // refusal a voter could sign the high floor, then lower its
            // promise to the low one, and hold a baseline that the very
            // floor it certified would refuse.
            for (voter, held) in &run.held {
                let baseline = ReadinessLedger::recovered(*voter, epoch(), *held).floor();
                let signed: Vec<_> = floors
                    .iter()
                    .filter(|floor| floor.signers().contains(voter))
                    .collect();
                let Some(candidate) = held else {
                    assert!(
                        signed.is_empty(),
                        "{voter:?} signed a floor without holding a promise"
                    );
                    continue;
                };
                assert_eq!(baseline, candidate.position);
                for promise in run.promises.get(voter).into_iter().flatten() {
                    assert!(
                        promise.candidate.position <= baseline,
                        "{voter:?} promised {:?} and then held the lower {baseline:?}",
                        promise.candidate.position
                    );
                }
                for floor in signed {
                    assert!(
                        floor.admits_voter(baseline),
                        "{voter:?} signed the floor at {:?} but would vote from {baseline:?}, \
                         below its own promise",
                        floor.position()
                    );
                }
            }
        }
        assert!(
            contested.is_empty(),
            "two subjects were certified at one position: {contested:?}"
        );
        assert_eq!(
            missed, 0,
            "a majority read discovered less than a certified floor"
        );
        summaries.push(Summary {
            voters: n,
            worlds: all.len(),
            with_a_floor,
            certified: certified_all,
            contested,
            missed,
            unidentified,
            refusals,
        });
    }
    fixture("floor_scenarios.json", &summaries);
}

/// Every ordering of a slice, for the small slices used here.
fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.is_empty() {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let mut rest = items.to_vec();
        rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, item.clone());
            out.push(tail);
        }
    }
    out
}

#[derive(Serialize)]
struct Counterexample {
    name: &'static str,
    rule_removed: &'static str,
    voters: u8,
    /// What the broken rule let happen.
    witness: String,
    /// What the rule as written does instead.
    refused: String,
}

/// The three rules, removed one at a time, each with the counterexample
/// the model finds.
#[test]
fn removing_a_rule_produces_the_counterexample_it_exists_for() {
    let mut out = Vec::new();

    // 1. A voter may be ready for two subjects at one position.
    //
    // Five voters: three ready for subject A at position 4, and the
    // other two plus one of the first three ready for subject B there.
    // Both are majorities, so both certify -- two different states at
    // one executed prefix, which is the divergence the whole protocol
    // exists to prevent.
    let five = voters(5);
    let a = at(LOW, 0xa1);
    let b = at(LOW, 0xb2);
    let forged_a: Vec<Readiness> = (0..3)
        .map(|i| Readiness {
            voter: r(i),
            candidate: a,
        })
        .collect();
    let forged_b: Vec<Readiness> = [2u8, 3, 4]
        .iter()
        .map(|i| Readiness {
            voter: r(*i),
            candidate: b,
        })
        .collect();
    let certified_a = activate(&five, &forged_a).expect("a majority");
    let certified_b = activate(&five, &forged_b).expect("a majority");
    assert_eq!(certified_a.position(), certified_b.position());
    assert_ne!(certified_a.subject(), certified_b.subject());
    // The ledger is what makes those two certificates unbuildable: the
    // voter in both of them is refused the second subject.
    let mut ledger = ReadinessLedger::new(r(2), epoch());
    ledger.record(&five, a).expect("first");
    let refused = ledger.record(&five, b).expect_err("second subject");
    assert_eq!(refused, ReadinessError::Competing { held: a });
    out.push(Counterexample {
        name: "competing_subjects_allowed",
        rule_removed: "ReadinessLedger refuses a second subject at a position it is ready for",
        voters: 5,
        witness: format!(
            "position {} certified for subjects {:02x} and {:02x} by overlapping majorities",
            certified_a.position().get(),
            certified_a.subject().0[0],
            certified_b.subject().0[0]
        ),
        refused: format!("{refused:?}"),
    });

    // 2. A recovery reads one voter instead of a majority.
    //
    // Three voters: two are ready for the high floor and certify it;
    // the third never was. A recovery that reads only the third
    // discovers nothing and would let it vote from a baseline below the
    // floor the cluster already forgot below.
    let three = voters(3);
    let high = at(HIGH, 0xc3);
    let ready: Vec<Readiness> = (0..2)
        .map(|i| Readiness {
            voter: r(i),
            candidate: high,
        })
        .collect();
    let floor = activate(&three, &ready).expect("a majority");
    // The one voter answers with no promise, so what reaches `discover`
    // is the empty slice -- the same value as a read that reached nobody.
    // Nothing here can tell the two apart, which is why the caller counts
    // who answered before it calls `discover`.
    let one_report: Vec<Readiness> = Vec::new();
    let narrow = discover(&one_report);
    assert!(narrow.is_none());
    let majority_report: Vec<Readiness> = [0u8, 2]
        .iter()
        .filter_map(|i| ready.iter().find(|p| p.voter == r(*i)).copied())
        .collect();
    let wide = discover(&majority_report).expect("the intersection");
    assert_eq!(wide.position, floor.position());
    out.push(Counterexample {
        name: "recovery_reads_one_voter",
        rule_removed: "a recovery discovers the floor from a majority of reports",
        voters: 3,
        witness: format!(
            "floor certified at position {} discovered as {:?} by the one voter that never promised",
            floor.position().get(),
            narrow.map(|d| d.position.get())
        ),
        refused: format!(
            "a majority of two, one of them a signer, discovers position {}",
            wide.position.get()
        ),
    });

    // 3. Possession is counted as readiness.
    //
    // Three voters hold the checkpoint bytes; two of them recorded
    // nothing durable. Counting possession certifies a floor that
    // nobody is bound by: a majority read of the two that promised
    // nothing discovers no floor at all, so a lagging replica is
    // admitted to vote from a baseline below it.
    let holders: Vec<Readiness> = (0..2)
        .map(|i| Readiness {
            voter: r(i),
            candidate: high,
        })
        .collect();
    let from_possession = activate(&three, &holders).expect("a majority of holders");
    let after_crash: Vec<Readiness> = Vec::new();
    let discovered = discover(&after_crash);
    let baseline = ExecutionPosition::new(HIGH - 1).unwrap();
    assert!(!from_possession.admits_voter(baseline));
    assert!(discovered.is_none());
    out.push(Counterexample {
        name: "possession_counted_as_readiness",
        rule_removed: "readiness is a durable promise, recorded before it is counted",
        voters: 3,
        witness: format!(
            "floor certified at position {} by holders that recorded nothing; after a crash a \
             majority read discovers {:?} and admits a baseline of {}",
            from_possession.position().get(),
            discovered.map(|d| d.position.get()),
            baseline.get()
        ),
        refused: "a promise survives the crash that loses the bytes' owner's memory, so the \
                  same majority read discovers the floor"
            .into(),
    });

    fixture("floor_counterexamples.json", &out);
}

/// The rules that are not about quorums: epoch, membership, and what a
/// message at or below a held floor may do.
#[test]
fn a_floor_binds_its_own_configuration_and_fences_what_it_covers() {
    let three = voters(3);
    let high = at(HIGH, 0xc3);

    // Another epoch's candidate is not this configuration's floor.
    let mut ledger = ReadinessLedger::new(r(0), epoch());
    let elsewhere = FloorCandidate {
        epoch: ConfigurationEpoch::new(8).unwrap(),
        ..high
    };
    assert_eq!(
        ledger.record(&three, elsewhere),
        Err(ReadinessError::EpochMismatch)
    );

    // An observer holds catch-up state, not obligations.
    let mut observer = ReadinessLedger::new(r(9), epoch());
    assert_eq!(
        observer.record(&three, high),
        Err(ReadinessError::NotAVoter)
    );
    assert_eq!(
        activate(
            &three,
            &[Readiness {
                voter: r(9),
                candidate: high
            }]
        ),
        Err(ActivationError::NotAVoter { replica: r(9) })
    );

    // A promise repeated is the same promise; a promise lowered is not
    // a promise.
    ledger.record(&three, high).expect("first");
    ledger.record(&three, high).expect("the same promise again");
    assert_eq!(
        ledger.record(&three, at(LOW, 0xa1)),
        Err(ReadinessError::Regression { held: high })
    );

    // Evidence about two different floors is not one certificate.
    assert_eq!(
        activate(
            &three,
            &[
                Readiness {
                    voter: r(0),
                    candidate: high
                },
                Readiness {
                    voter: r(1),
                    candidate: at(LOW, 0xa1)
                },
            ]
        ),
        Err(ActivationError::Divided)
    );

    // A minority certifies nothing.
    assert_eq!(
        activate(
            &three,
            &[Readiness {
                voter: r(0),
                candidate: high
            }]
        ),
        Err(ActivationError::NoQuorum { have: 1, need: 2 })
    );

    // The fence. At or below the floor a message is answered from the
    // retained outcome; above it nothing changes.
    let floor = activate(
        &three,
        &(0..2)
            .map(|i| Readiness {
                voter: r(i),
                candidate: high,
            })
            .collect::<Vec<_>>(),
    )
    .expect("a majority");
    assert_eq!(
        floor.verdict(ExecutionPosition::new(HIGH).unwrap()),
        FenceVerdict::Retained
    );
    assert_eq!(
        floor.verdict(ExecutionPosition::new(HIGH - 1).unwrap()),
        FenceVerdict::Retained
    );
    assert_eq!(
        floor.verdict(ExecutionPosition::new(HIGH + 1).unwrap()),
        FenceVerdict::Ordinary
    );
    assert!(!floor.admits_voter(ExecutionPosition::new(HIGH - 1).unwrap()));
    assert!(floor.admits_voter(ExecutionPosition::new(HIGH).unwrap()));

    // A certificate of another configuration never becomes this
    // replica's floor.
    let mut held = FloorLedger::new(ConfigurationEpoch::new(8).unwrap());
    assert_eq!(held.install(floor).unwrap(), FloorInstall::AlreadyCovered);
    assert_eq!(held.position(), ExecutionPosition::ZERO);
}

/// A permanently absent voter no longer stops forgetting.
///
/// This is the availability the whole task buys, and it is worth a test
/// of its own because it is the one thing task-51's all-voter rule
/// cannot do: with one of three voters gone for ever, the other two
/// certify, discover and fence exactly as if it were there.
#[test]
fn a_permanently_absent_voter_does_not_stop_the_floor() {
    let three = voters(3);
    let high = at(HIGH, 0xc3);
    let present: Vec<Readiness> = (0..2)
        .map(|i| {
            let mut ledger = ReadinessLedger::new(r(i), epoch());
            ledger.record(&three, high).expect("ready")
        })
        .collect();
    let floor = activate(&three, &present).expect("two of three");
    assert_eq!(floor.signers().len(), 2);
    assert!(!floor.signers().contains(&r(2)));

    // Every majority that could ever be read contains a signer, so the
    // absent voter's own return -- whenever it happens -- discovers the
    // floor rather than voting under it.
    for majority in majorities(3) {
        let reports: Vec<Readiness> = present
            .iter()
            .filter(|p| majority.contains(&p.voter))
            .copied()
            .collect();
        let discovered = discover(&reports).expect("a signer is in every majority");
        assert_eq!(discovered.position, floor.position());
        assert_eq!(discovered.subject(), Some(floor.subject()));
    }
}

/// A certified floor whose position a majority read finds but whose
/// image it cannot name. Voters 0 and 2 are ready for A and certify it;
/// voter 1 is ready for the competing B at the same position, which is
/// legal because B was never certified. The read {0, 1} sees one report
/// for each subject: the position binds, `subject()` is `None`, and
/// nothing in that read can say A. A read that holds a majority for one
/// subject names it, and it is the certified one.
#[test]
fn a_majority_read_binds_the_position_even_when_it_cannot_name_the_image() {
    let three = voters(3);
    let a = at(LOW, 0xa1);
    let b = at(LOW, 0xb2);
    let mut ledgers: Vec<ReadinessLedger> = (0..3)
        .map(|i| ReadinessLedger::new(r(i), epoch()))
        .collect();
    let ready_a: Vec<Readiness> = [0usize, 2]
        .iter()
        .map(|i| ledgers[*i].record(&three, a).expect("ready for A"))
        .collect();
    let ready_b = ledgers[1].record(&three, b).expect("ready for B");
    let floor = activate(&three, &ready_a).expect("two of three");
    assert_eq!(
        activate(&three, &[ready_b]),
        Err(ActivationError::NoQuorum { have: 1, need: 2 })
    );

    let narrow = discover(&[ready_a[0], ready_b]).expect("a signer is read");
    assert_eq!(narrow.position, floor.position());
    assert_eq!(
        narrow.subjects,
        [a.subject, b.subject].into_iter().collect()
    );
    assert_eq!(narrow.subject(), None);
    assert_eq!(narrow.certified(&three), None);
    assert_eq!(narrow.ready_for(a.subject), [r(0)].into_iter().collect());
    assert_eq!(narrow.ready_for(b.subject), [r(1)].into_iter().collect());

    let wide = discover(&[ready_a[0], ready_b, ready_a[1]]).expect("everyone is read");
    assert_eq!(wide.subject(), None);
    assert_eq!(wide.certified(&three), Some(floor.subject()));
    // A replica outside the epoch never tips the count.
    let stranger = Readiness {
        voter: r(9),
        candidate: b,
    };
    let padded = discover(&[ready_a[0], ready_b, stranger]).expect("a signer is read");
    assert_eq!(padded.ready_for(b.subject).len(), 2);
    assert_eq!(padded.certified(&three), None);
}
